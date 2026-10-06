//! Pagination response headers (`Link`, `X-Paging-*`) shared by all list endpoints.

use axum::http::HeaderMap;
use axum::http::header::LINK;
use tads_store::Page;

/// Build the paging headers for a page of `count` items. `base_path` is the request path (e.g.
/// `/tracks`) used to render the `Link rel="next"` cursor URL. Extra flags (`reverse_order`,
/// `timerange`) are appended by the events/delete-requests handlers via [`PagingHeaders`].
pub struct PagingHeaders(HeaderMap);

impl PagingHeaders {
    pub fn new<T>(page: &Page<T>, base_path: &str) -> Self {
        let mut headers = HeaderMap::new();
        insert(&mut headers, "X-Paging-Limit", &page.limit.to_string());
        insert(
            &mut headers,
            "X-Paging-Count",
            &page.items.len().to_string(),
        );

        if let Some(next) = &page.next_key {
            insert(&mut headers, "X-Paging-NextKey", next);
            let link = format!(
                "<{base_path}?page={next}&limit={}>; rel=\"next\"",
                page.limit
            );
            if let Ok(value) = link.parse() {
                headers.insert(LINK, value);
            }
        }
        Self(headers)
    }

    /// Append `X-Paging-Reverse-Order` (events + delete-requests listings).
    pub fn with_reverse_order(mut self, reverse: bool) -> Self {
        insert(&mut self.0, "X-Paging-Reverse-Order", &reverse.to_string());
        self
    }

    /// Append `X-Paging-Timerange` (events listing).
    pub fn with_timerange(mut self, timerange: &str) -> Self {
        insert(&mut self.0, "X-Paging-Timerange", timerange);
        self
    }

    pub fn into_header_map(self) -> HeaderMap {
        self.0
    }
}

fn insert(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(v) = value.parse() {
        headers.insert(name, v);
    }
}
