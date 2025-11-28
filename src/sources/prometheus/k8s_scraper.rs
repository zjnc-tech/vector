use std::time::Duration;
use http::Uri;
use hyper::Body;
use vector_lib::event::Event;

use super::k8s_discovery::{
    K8sServiceDiscovery,
    KubernetesSdConfig,
    MetadataLabelsConfig,
    DiscoveredTarget,
};
use super::parser;
use super::metrics_enrichment::add_metadata_to_metric;
use crate::config::ProxyConfig;
use crate::http::{Auth, HttpClient, QueryParameters};
use crate::sources::util::http_client::build_url;
use crate::tls::TlsSettings;
use crate::SourceSender;

/// Kubernetes 服务发现抓取器
pub struct K8sScraper {
    discovery: K8sServiceDiscovery,
    client: HttpClient,
    auth: Option<Auth>,
    query_params: QueryParameters,
    timeout: Duration,
    honor_labels: bool,
    instance_tag: Option<String>,
    endpoint_tag: Option<String>,
    metadata_config: MetadataLabelsConfig,
}

impl K8sScraper {
    pub async fn new(
        k8s_config: KubernetesSdConfig,
        tls_config: Option<crate::tls::TlsConfig>,
        auth: Option<Auth>,
        query_params: QueryParameters,
        timeout: Duration,
        honor_labels: bool,
        instance_tag: Option<String>,
        endpoint_tag: Option<String>,
        proxy: &ProxyConfig,
    ) -> crate::Result<Self> {
        let metadata_config = k8s_config.metadata_labels.clone();
        let discovery = K8sServiceDiscovery::new(k8s_config).await?;
        
        let tls = TlsSettings::from_options(tls_config.as_ref())?;
        let client = HttpClient::new(tls, proxy)?;
        
        Ok(Self {
            discovery,
            client,
            auth,
            query_params,
            timeout,
            honor_labels,
            instance_tag,
            endpoint_tag,
            metadata_config,
        })
    }
    
    /// 运行抓取循环
    pub async fn run(
        self,
        interval: Duration,
        mut shutdown: crate::shutdown::ShutdownSignal,
        mut out: SourceSender,
    ) -> Result<(), ()> {
        let mut interval_timer = tokio::time::interval(interval);
        
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("Shutting down Kubernetes scraper");
                    break;
                }
                _ = interval_timer.tick() => {
                    let targets = self.discovery.get_targets().await;
                    
                    if targets.is_empty() {
                        warn!("No Kubernetes targets discovered");
                        continue;
                    }
                    
                    info!("Scraping {} Kubernetes targets", targets.len());
                    
                    let futures: Vec<_> = targets.into_iter().map(|target| {
                        Self::scrape_target(
                            target,
                            self.client.clone(),
                            self.auth.clone(),
                            self.query_params.clone(),
                            self.timeout,
                            self.honor_labels,
                            self.instance_tag.clone(),
                            self.endpoint_tag.clone(),
                            self.metadata_config.clone(),
                        )
                    }).collect();
                    
                    let results = futures::future::join_all(futures).await;
                    
                    for events in results {
                        if !events.is_empty() {
                            if let Err(e) = out.send_batch(events).await {
                                error!("Failed to send events: {:?}", e);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// 抓取单个 target
    async fn scrape_target(
        target: DiscoveredTarget,
        client: HttpClient,
        auth: Option<Auth>,
        query_params: QueryParameters,
        timeout: Duration,
        honor_labels: bool,
        instance_tag: Option<String>,
        endpoint_tag: Option<String>,
        metadata_config: MetadataLabelsConfig,
    ) -> Vec<Event> {
        let uri = match target.url.parse::<Uri>() {
            Ok(u) => build_url(&u, &query_params),
            Err(e) => {
                error!("Invalid URL {}: {:?}", target.url, e);
                return Vec::new();
            }
        };
        
        let mut request = hyper::Request::builder()
            .method("GET")
            .uri(uri.clone())
            .header("Accept", "text/plain");
        
        if let Some(ref auth) = auth {
            request = auth.apply_builder(request);
        }
        
        let request = match request.body(Body::empty()) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to build request: {:?}", e);
                return Vec::new();
            }
        };
        
        let response = match tokio::time::timeout(timeout, client.send(request)).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                error!("HTTP error for {}: {:?}", uri, e);
                return Vec::new();
            }
            Err(_) => {
                error!("Timeout for {}", uri);
                return Vec::new();
            }
        };
        
        let (parts, body) = response.into_parts();
        if parts.status != hyper::StatusCode::OK {
            error!("HTTP {} from {}", parts.status, uri);
            return Vec::new();
        }
        
        let body_bytes = match hyper::body::to_bytes(body).await {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to read body: {:?}", e);
                return Vec::new();
            }
        };
        
        let body_str = String::from_utf8_lossy(&body_bytes);
        let mut events = match parser::parse_text(&body_str) {
            Ok(e) => e,
            Err(e) => {
                error!("Failed to parse metrics from {}: {:?}", uri, e);
                return Vec::new();
            }
        };
        
        for event in &mut events {
            let metric = event.as_mut_metric();
            
            if let Some(ref tag) = instance_tag {
                let instance = format!("{}:{}", 
                    uri.host().unwrap_or_default(),
                    uri.port_u16().unwrap_or(80)
                );
                if !honor_labels || metric.tag_value(tag).is_none() {
                    metric.replace_tag(tag.clone(), instance);
                }
            }
            
            if let Some(ref tag) = endpoint_tag {
                let endpoint = uri.to_string();
                if !honor_labels || metric.tag_value(tag).is_none() {
                    metric.replace_tag(tag.clone(), endpoint);
                }
            }
            
            add_metadata_to_metric(metric, &target, &metadata_config, honor_labels);
        }
        
        events
    }
}