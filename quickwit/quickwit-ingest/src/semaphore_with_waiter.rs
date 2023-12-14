// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};

/// [`SemaphoreWithMaxWaiters`] is an extension of semaphore
/// that limits the number of waiters.
///
/// If more than n-waiters then acquire returns an error.
pub struct SemaphoreWithMaxWaiters {
    permits: Semaphore,
    num_waiters: AtomicUsize,
    max_num_waiters: usize,
}

impl SemaphoreWithMaxWaiters {
    /// Creates a new [`SemaphoreWithMaxWaiters`].
    pub fn new(num_permits: usize, max_num_waiters: usize) -> Self {
        Self {
            permits: Semaphore::new(num_permits),
            num_waiters: AtomicUsize::new(0),
            max_num_waiters,
        }
    }

    /// Acquires a permit.
    pub async fn acquire(&self) -> Result<SemaphorePermit<'_>, ()> {
        match self.permits.try_acquire() {
            Ok(permit) => {
                return Ok(permit);
            }
            Err(TryAcquireError::NoPermits) => {}
            Err(TryAcquireError::Closed) => {
                // The `permits` semaphore should never be closed because we don't expose the
                // `Semaphore::close` API and never call it internally.
                panic!("semaphore should not be closed");
            }
        };
        if self.num_waiters.load(Ordering::Acquire) >= self.max_num_waiters {
            return Err(());
        }
        self.num_waiters.fetch_add(1, Ordering::Release);
        let permit = self
            .permits
            .acquire()
            .await
            .expect("semaphore should not be closed"); // (See justification above)
        self.num_waiters.fetch_sub(1, Ordering::Release);
        Ok(permit)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_semaphore_max_waiters() {
        let semaphore_with_waiters = Arc::new(super::SemaphoreWithMaxWaiters::new(1, 1));
        let permit = semaphore_with_waiters.acquire().await.unwrap();
        let semaphore_with_waiters_clone = semaphore_with_waiters.clone();
        let join_handle = tokio::task::spawn(async move {
            let _ = semaphore_with_waiters_clone.acquire().await;
        });
        assert!(!join_handle.is_finished());
        tokio::time::sleep(Duration::from_millis(500)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(semaphore_with_waiters.acquire().await.is_err());
        assert!(!join_handle.is_finished());
        drop(permit);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(join_handle.await.is_ok());
        assert!(semaphore_with_waiters.acquire().await.is_ok());
    }
}
