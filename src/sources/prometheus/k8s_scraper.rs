use futures::pin_mut;
use futures::stream;
use futures::StreamExt;
use http::{
    header::{ACCEPT_ENCODING, CONTENT_ENCODING},
    Uri,
};
use hyper::Body;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{watch, Mutex, Notify};
use vector_lib::event::metric::{MetricKind, MetricValue};
use vector_lib::event::{Event, Metric};
use vector_lib::internal_event::{ComponentEventsDropped, INTENTIONAL};
use vector_lib::json_size::JsonSize;

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

#[derive(Debug)]
struct ScrapedBatch {
    target_url: String,
    events: Vec<Event>,
    event_count: usize,
    event_bytes: JsonSize,
}

#[derive(Debug)]
struct QueuedBatch {
    batch: ScrapedBatch,
    enqueued_at: Instant,
}

#[derive(Debug)]
struct BatchQueue {
    capacity: AtomicUsize,
    items: Mutex<VecDeque<QueuedBatch>>,
    notify: Notify,
}

impl BatchQueue {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: AtomicUsize::new(capacity),
            items: Mutex::new(VecDeque::with_capacity(capacity)),
            notify: Notify::new(),
        }
    }

    fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    fn set_capacity(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Relaxed);
    }

    async fn push(&self, batch: ScrapedBatch) -> Option<QueuedBatch> {
        let mut items = self.items.lock().await;
        let capacity = self.capacity();
        let evicted = if items.len() >= capacity {
            items.pop_front()
        } else {
            None
        };
        items.push_back(QueuedBatch {
            batch,
            enqueued_at: Instant::now(),
        });
        drop(items);

        self.notify.notify_one();
        evicted
    }

    async fn pop(&self, mut shutdown: crate::shutdown::ShutdownSignal) -> Option<QueuedBatch> {
        let mut shutdown_seen = false;

        loop {
            if let Some(batch) = self.try_pop().await {
                return Some(batch);
            }

            if shutdown_seen {
                return None;
            }

            tokio::select! {
                _ = self.notify.notified() => {},
                _ = &mut shutdown => {
                    shutdown_seen = true;
                }
            }
        }
    }

    async fn try_pop(&self) -> Option<QueuedBatch> {
        let mut items = self.items.lock().await;
        items.pop_front()
    }
}

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
    send_queue_capacity: Option<NonZeroUsize>,
    send_worker_count: Option<NonZeroUsize>,
    scrape_concurrency: Option<NonZeroUsize>,
    send_timeout: Duration,
}

impl K8sScraper {
    fn default_send_queue_capacity(target_count: usize) -> NonZeroUsize {
        let capacity = match target_count {
            0..=500 => 16,
            501..=1_000 => 24,
            _ => 32,
        };

        NonZeroUsize::new(capacity).expect("static")
    }

    fn default_scrape_concurrency(target_count: usize) -> NonZeroUsize {
        let concurrency = match target_count {
            0..=500 => 32,
            501..=1_000 => 96,
            _ => 192,
        };

        NonZeroUsize::new(concurrency).expect("static")
    }

    fn default_send_worker_count(target_count: usize) -> NonZeroUsize {
        let workers = match target_count {
            0..=1_000 => 4,
            _ => 6,
        };

        NonZeroUsize::new(workers).expect("static")
    }

    fn effective_send_queue_capacity(&self, target_count: usize) -> NonZeroUsize {
        self.send_queue_capacity
            .unwrap_or_else(|| Self::default_send_queue_capacity(target_count))
    }

    fn effective_send_worker_count(&self, target_count: usize) -> NonZeroUsize {
        self.send_worker_count
            .unwrap_or_else(|| Self::default_send_worker_count(target_count))
    }

    fn effective_scrape_concurrency(&self, target_count: usize) -> NonZeroUsize {
        self.scrape_concurrency
            .unwrap_or_else(|| Self::default_scrape_concurrency(target_count))
    }

    fn send_worker_spawn_limit(&self) -> usize {
        self.send_worker_count.map_or(6, |count| count.get())
    }

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
        let send_queue_capacity = k8s_config.send_queue_capacity;
        let send_worker_count = k8s_config.send_worker_count;
        let scrape_concurrency = k8s_config.scrape_concurrency;
        let send_timeout = Duration::from_secs(k8s_config.send_timeout_secs.get() as u64);
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
            send_queue_capacity,
            send_worker_count,
            scrape_concurrency,
            send_timeout,
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
        let initial_targets = self.discovery.get_targets().await;
        let initial_target_count = initial_targets.len();
        let initial_limits = (
            self.effective_send_queue_capacity(initial_target_count),
            self.effective_send_worker_count(initial_target_count),
            self.effective_scrape_concurrency(initial_target_count),
        );
        let queue = Arc::new(BatchQueue::new(initial_limits.0.get()));
        let mut send_worker_handles = Vec::with_capacity(self.send_worker_spawn_limit());
        let (send_worker_tx, send_worker_rx) = watch::channel(initial_limits.1.get());

        for worker_index in 0..self.send_worker_spawn_limit() {
            let queue = Arc::clone(&queue);
            let worker_shutdown = shutdown.clone();
            let worker_out = out.clone();
            let worker_desired = send_worker_rx.clone();
            let send_timeout = self.send_timeout;
            send_worker_handles.push(tokio::spawn(Self::run_send_worker(
                worker_index,
                worker_desired,
                queue,
                worker_shutdown,
                worker_out,
                send_timeout,
            )));
        }

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("Shutting down Kubernetes scraper");
                    break;
                }
                _ = interval_timer.tick() => {
                    let targets = self.discovery.get_targets().await;
                    let target_count = targets.len();
                    let current_limits = (
                        self.effective_send_queue_capacity(target_count),
                        self.effective_send_worker_count(target_count),
                        self.effective_scrape_concurrency(target_count),
                    );
                    queue.set_capacity(current_limits.0.get());
                    let _ = send_worker_tx.send(current_limits.1.get());
                    let scrape_concurrency = current_limits.2.get();
                    if targets.is_empty() {
                        warn!("No Kubernetes targets discovered");
                        continue;
                    }

                    info!(
                        target_count = target_count,
                        "Discovered Kubernetes targets"
                    );

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
                    .buffer_unordered(scrape_concurrency);
                    pin_mut!(scrape_stream);
                    while let Some(scraped_batch) = scrape_stream.next().await {
                        if let Some(evicted) = queue.push(scraped_batch).await {
                            emit!(ComponentEventsDropped::<INTENTIONAL> {
                                count: evicted.batch.event_count,
                                reason: "Kubernetes scrape send queue full; evicting oldest batch.",
                            });

                            warn!(
                                target = %evicted.batch.target_url,
                                events = evicted.batch.event_count,
                                bytes = ?evicted.batch.event_bytes,
                                queue_capacity = queue.capacity(),
                                internal_log_rate_limit = true,
                                "Dropping oldest batch because send queue is full"
                            );
                        }
                    }
                }
            }
        }

        for handle in send_worker_handles {
            if let Err(e) = handle.await {
                error!("Send worker task failed: {:?}", e);
            }
        }

        Ok(())
    }

    async fn run_send_worker(
        worker_index: usize,
        mut desired_workers: watch::Receiver<usize>,
        queue: Arc<BatchQueue>,
        shutdown: crate::shutdown::ShutdownSignal,
        mut out: SourceSender,
        send_timeout: Duration,
    ) {
        loop {
            while worker_index >= *desired_workers.borrow() {
                tokio::select! {
                    changed = desired_workers.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                    _ = shutdown.clone() => return,
                }
            }

            let Some(QueuedBatch { batch, enqueued_at }) = queue.pop(shutdown.clone()).await else {
                break;
            };

            let queue_wait = enqueued_at.elapsed();
            let send_started = Instant::now();

            match tokio::time::timeout(
                send_timeout,
                out.send_event_stream(stream::iter(batch.events)),
            )
            .await
            {
                Ok(Ok(())) => {
                    let send_elapsed = send_started.elapsed();
                    if queue_wait >= Duration::from_secs(2)
                        || send_elapsed >= Duration::from_secs(2)
                    {
                        warn!(
                            target = %batch.target_url,
                            events = batch.event_count,
                            bytes = ?batch.event_bytes,
                            queue_wait = ?queue_wait,
                            elapsed = ?send_elapsed,
                            "send worker processed batch slowly"
                        );
                    }
                }
                Ok(Err(e)) => {
                    error!("Failed to send events for {}: {:?}", batch.target_url, e);
                }
                Err(_) => {
                    let elapsed = send_started.elapsed();
                    warn!(
                        target = %batch.target_url,
                        events = batch.event_count,
                        bytes = ?batch.event_bytes,
                        timeout = ?send_timeout,
                        elapsed = ?elapsed,
                        "send_event_stream timed out; dropping remaining batch"
                    );
                }
            }
        }
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
    ) -> ScrapedBatch {
        let target_url = target.url.clone();
        let scrape_started = Instant::now();

        let uri = match target.url.parse::<Uri>() {
            Ok(u) => build_url(&u, &query_params),
            Err(e) => {
                error!("Invalid URL {}: {:?}", target.url, e);
                let events = vec![Self::build_up_event(&target, &metadata_config, 0).await];
                return ScrapedBatch {
                    target_url,
                    event_count: events.len(),
                    event_bytes: events.estimated_json_encoded_size_of(),
                    events,
                };
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
                error!(
                    target = %uri,
                    request_elapsed = ?request_started.elapsed(),
                    total_elapsed = ?scrape_started.elapsed(),
                    "Failed to build request: {:?}",
                    e
                );
            })?;

            let response_started = Instant::now();
            let response = client.send(request).await.map_err(|e| {
                error!(
                    target = %uri,
                    request_elapsed = ?request_started.elapsed(),
                    total_elapsed = ?scrape_started.elapsed(),
                    response_elapsed = ?response_started.elapsed(),
                    "HTTP error: {:?}",
                    e
                );
            })?;
            let request_elapsed = request_started.elapsed();

            let (parts, body) = response.into_parts();
            if parts.status != hyper::StatusCode::OK {
                error!(
                    target = %uri,
                    request_elapsed = ?request_elapsed,
                    total_elapsed = ?scrape_started.elapsed(),
                    status = %parts.status,
                    "HTTP response status was not OK"
                );
                return Err(());
            }

            let body_started = Instant::now();
            let body_bytes = hyper::body::to_bytes(body).await.map_err(|e| {
                error!(
                    target = %uri,
                    request_elapsed = ?request_elapsed,
                    body_elapsed = ?body_started.elapsed(),
                    total_elapsed = ?scrape_started.elapsed(),
                    "Failed to read body: {:?}",
                    e
                );
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
                error!(
                    target = %uri,
                    request_elapsed = ?request_elapsed,
                    body_elapsed = ?body_elapsed,
                    encoded_bytes = body_encoded_bytes,
                    total_elapsed = ?scrape_started.elapsed(),
                    "Failed to decode body: {}",
                    e
                );
            })?;
            let decode_elapsed = decode_started.elapsed();

            let parse_started = Instant::now();
            let body_str = String::from_utf8_lossy(&body_bytes);
            let events = parser::parse_text(&body_str).map_err(|e| {
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
            })?;

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

            Ok::<_, ()>(events)
        };

        let events = match tokio::time::timeout(timeout, scrape).await {
            Ok(Ok(events)) => events,
            Ok(Err(_)) => {
                vec![Self::build_up_event(&target, &metadata_config, 0).await]
            }
            Err(_) => {
                error!(
                    target = %uri,
                    total_elapsed = ?scrape_started.elapsed(),
                    timeout = ?timeout,
                    "Timeout while scraping (request, body read, parse, or enrichment exceeded budget)"
                );
                vec![Self::build_up_event(&target, &metadata_config, 0).await]
            }
        };

        ScrapedBatch {
            target_url,
            event_count: events.len(),
            event_bytes: events.estimated_json_encoded_size_of(),
            events,
        }
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
