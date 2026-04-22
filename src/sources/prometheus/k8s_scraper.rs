use futures::pin_mut;
use futures::stream::{self, FuturesUnordered};
use futures::StreamExt;
use http::{
    header::{ACCEPT_ENCODING, CONTENT_ENCODING},
    Uri,
};
use hyper::Body;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use vector_lib::event::metric::{MetricKind, MetricValue};
use vector_lib::event::{Event, Metric};

use crate::config::ProxyConfig;
use crate::http::{Auth, HttpClient, QueryParameters};
use crate::sources::util::decode;
use crate::sources::util::http_client::build_url;
use crate::tls::TlsSettings;
use crate::SourceSender;

use super::k8s_discovery::{
    DiscoveredTarget, K8sServiceDiscovery, KubernetesSdConfig, MetadataLabelsConfig,
};
use super::metrics_enrichment::add_metadata_to_metric;
use super::parser;
use vector_lib::EstimatedJsonEncodedSizeOf;

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
    send_batch_buffer: NonZeroUsize,
    scrape_concurrency: NonZeroUsize,
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
        let send_batch_buffer = k8s_config.send_batch_buffer;
        let scrape_concurrency = k8s_config.scrape_concurrency;
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
            send_batch_buffer,
            scrape_concurrency,
        })
    }

    pub async fn run(
        self,
        interval: Duration,
        mut shutdown: crate::shutdown::ShutdownSignal,
        out: SourceSender,
    ) -> Result<(), ()> {
        let mut interval_timer = tokio::time::interval(interval);
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay); // 如果错过了一个周期，就往后顺延，避免积压抓取任务

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("Shutting down Kubernetes scraper");
                    break;
                }
                _ = interval_timer.tick() => {
                    let targets = self.discovery.get_targets().await;
                    let log_kubelet = targets
                        .iter()
                        .any(|target| target.job_name == "vector_kubelet_scrape");

                    if log_kubelet {
                        info!("targets scrape tick start");
                    }

                    if targets.is_empty() {
                        if log_kubelet {
                            warn!("No Kubernetes targets discovered");
                        }
                        continue;
                    }

                    info!("Scraping {} Kubernetes targets", targets.len());
                    let scrape_stream = stream::iter(targets.into_iter().map(|target| {
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
                    }))
                    .buffer_unordered(self.scrape_concurrency.get());
                    pin_mut!(scrape_stream);
                    let mut send_futs: FuturesUnordered<_> = FuturesUnordered::new();
                    let mut scrape_done = false;
                    let mut in_flight_sends = 0usize;
                    let max_in_flight_sends = self.send_batch_buffer.get();

                    while !scrape_done || !send_futs.is_empty() {
                        tokio::select! {
                            maybe_scraped = scrape_stream.next(), if !scrape_done && in_flight_sends < max_in_flight_sends => {
                                match maybe_scraped {
                                    Some((target_url, events)) => {
                                        if !events.is_empty() {
                                            let event_count = events.len();
                                            let event_bytes = events.estimated_json_encoded_size_of();
                                            let mut out = out.clone();
                                            send_futs.push(async move {
                                                let send_started = Instant::now();
                                                let send_timeout = Duration::from_secs(60);
                                                match tokio::time::timeout(
                                                    send_timeout,
                                                    out.send_event_stream(stream::iter(events)),
                                                )
                                                .await
                                                {
                                                    Ok(Ok(())) => {
                                                        if log_kubelet {
                                                            let elapsed = send_started.elapsed();
                                                            if elapsed >= Duration::from_secs(2) {
                                                                warn!(
                                                                    target = %target_url,
                                                                    events = event_count,
                                                                    bytes = ?event_bytes,
                                                                    elapsed = ?elapsed,
                                                                    "send_event_stream is slow"
                                                                );
                                                            }
                                                        }
                                                    }
                                                    Ok(Err(e)) => {
                                                        error!(
                                                            "Failed to send events for {}: {:?}",
                                                            target_url,
                                                            e
                                                        );
                                                    }
                                                    Err(_) => {
                                                        let elapsed = send_started.elapsed();
                                                        warn!(
                                                            target = %target_url,
                                                            events = event_count,
                                                            bytes = ?event_bytes,
                                                            timeout = ?send_timeout,
                                                            elapsed = ?elapsed,
                                                            "send_event_stream timed out; dropping remaining batch"
                                                        );
                                                    }
                                                }
                                            });
                                            in_flight_sends += 1;
                                        }
                                    }
                                    None => {
                                        scrape_done = true;
                                    }
                                }
                            }
                            maybe_sent = send_futs.next(), if !send_futs.is_empty() => {
                                if maybe_sent.is_some() {
                                    in_flight_sends = in_flight_sends.saturating_sub(1);
                                }
                            }
                        }
                    }

                    if log_kubelet {
                        info!("targets scrape tick end");
                    }
                }
            }
        }

        Ok(())
    }

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
    ) -> (String, Vec<Event>) {
        let target_url = target.url.clone();
        let is_kubelet_job = target.job_name == "vector_kubelet_scrape";
        let scrape_started = Instant::now();

        let uri = match target.url.parse::<Uri>() {
            Ok(u) => build_url(&u, &query_params),
            Err(e) => {
                if is_kubelet_job {
                    error!("Invalid URL {}: {:?}", target.url, e);
                }
                return (
                    target_url,
                    vec![Self::build_up_event(&target, &metadata_config, 0).await],
                );
            }
        };

        let scrape = async {
            let request_started = Instant::now();
            let mut request = hyper::Request::builder()
                .method("GET")
                .uri(uri.clone())
                .header("Accept", "text/plain");

            if let Some(ref auth) = auth {
                request = auth.apply_builder(request);
            }

            request = request.header(ACCEPT_ENCODING, "gzip");

            let request = request.body(Body::empty()).map_err(|e| {
                if is_kubelet_job {
                    error!(
                        target = %uri,
                        request_elapsed = ?request_started.elapsed(),
                        total_elapsed = ?scrape_started.elapsed(),
                        "Failed to build request: {:?}",
                        e
                    );
                }
            })?;

            let response_started = Instant::now();
            let response = client.send(request).await.map_err(|e| {
                if is_kubelet_job {
                    error!(
                        target = %uri,
                        request_elapsed = ?request_started.elapsed(),
                        total_elapsed = ?scrape_started.elapsed(),
                        response_elapsed = ?response_started.elapsed(),
                        "HTTP error: {:?}",
                        e
                    );
                }
            })?;
            let request_elapsed = request_started.elapsed();

            let (parts, body) = response.into_parts();
            if parts.status != hyper::StatusCode::OK {
                if is_kubelet_job {
                    error!(
                        target = %uri,
                        request_elapsed = ?request_elapsed,
                        total_elapsed = ?scrape_started.elapsed(),
                        status = %parts.status,
                        "HTTP response status was not OK"
                    );
                }
                return Err(());
            }

            let body_started = Instant::now();
            let body_bytes = hyper::body::to_bytes(body).await.map_err(|e| {
                if is_kubelet_job {
                    error!(
                        target = %uri,
                        request_elapsed = ?request_elapsed,
                        body_elapsed = ?body_started.elapsed(),
                        total_elapsed = ?scrape_started.elapsed(),
                        "Failed to read body: {:?}",
                        e
                    );
                }
            })?;
            let body_elapsed = body_started.elapsed();
            let body_encoded_bytes = body_bytes.len();

            let decode_started = Instant::now();
            let body_bytes = decode(
                parts
                    .headers
                    .get(CONTENT_ENCODING)
                    .and_then(|value| value.to_str().ok()),
                body_bytes,
            )
            .map_err(|e| {
                if is_kubelet_job {
                    error!(
                        target = %uri,
                        request_elapsed = ?request_elapsed,
                        body_elapsed = ?body_elapsed,
                        encoded_bytes = body_encoded_bytes,
                        total_elapsed = ?scrape_started.elapsed(),
                        "Failed to decode body: {}",
                        e
                    );
                }
            })?;
            let decode_elapsed = decode_started.elapsed();
            let body_decoded_bytes = body_bytes.len();

            let parse_started = Instant::now();
            let body_str = String::from_utf8_lossy(&body_bytes);
            let events = parser::parse_text(&body_str).map_err(|e| {
                if is_kubelet_job {
                    error!(
                        target = %uri,
                        request_elapsed = ?request_elapsed,
                        body_elapsed = ?body_elapsed,
                        decode_elapsed = ?decode_elapsed,
                        parse_elapsed = ?parse_started.elapsed(),
                        total_elapsed = ?scrape_started.elapsed(),
                        "Failed to parse metrics: {:?}",
                        e
                    );
                }
            })?;
            let parse_elapsed = parse_started.elapsed();

            let enrich_started = Instant::now();

            let mut events = events;
            for event in &mut events {
                let metric = event.as_mut_metric();

                if let Some(ref tag) = instance_tag {
                    let instance = format!(
                        "{}:{}",
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

                add_metadata_to_metric(metric, &target, &metadata_config, honor_labels).await;
            }
            events.push(Self::build_up_event(&target, &metadata_config, 1).await);

            let enrich_elapsed = enrich_started.elapsed();

            if is_kubelet_job {
                info!(
                    target = %uri,
                    request_elapsed = ?request_elapsed,
                    body_elapsed = ?body_elapsed,
                    decode_elapsed = ?decode_elapsed,
                    parse_elapsed = ?parse_elapsed,
                    enrich_elapsed = ?enrich_elapsed,
                    encoded_bytes = body_encoded_bytes,
                    decoded_bytes = body_decoded_bytes,
                    events = events.len(),
                    total_elapsed = ?scrape_started.elapsed(),
                    "kubelet scrape timings"
                );
            }

            Ok::<_, ()>(events)
        };

        let events = match tokio::time::timeout(timeout, scrape).await {
            Ok(Ok(events)) => events,
            Ok(Err(_)) => {
                return (
                    target_url,
                    vec![Self::build_up_event(&target, &metadata_config, 0).await],
                );
            }
            Err(_) => {
                if is_kubelet_job {
                    error!(
                        target = %uri,
                        total_elapsed = ?scrape_started.elapsed(),
                        timeout = ?timeout,
                        "Timeout while scraping (request, body read, parse, or enrichment exceeded budget)"
                    );
                }
                return (
                    target_url,
                    vec![Self::build_up_event(&target, &metadata_config, 0).await],
                );
            }
        };

        (target_url, events)
    }

    async fn build_up_event(
        target: &DiscoveredTarget,
        metadata_config: &MetadataLabelsConfig,
        up: i64,
    ) -> Event {
        let mut metric = Metric::new(
            "up".to_string(),
            MetricKind::Absolute,
            MetricValue::Gauge { value: up as f64 },
        );

        add_metadata_to_metric(&mut metric, target, metadata_config, false).await;
        Event::from(metric)
    }
}
