//! Wall-clock boundary used by policy and deterministic scenario tests.

use std::fs;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use time::{Month, OffsetDateTime};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalDateTime {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
}

impl LocalDateTime {
    #[must_use]
    pub fn mmdd(self) -> u16 {
        u16::from(self.month) * 100 + u16::from(self.day)
    }

    #[must_use]
    pub fn date_key(self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

pub trait Clock {
    fn epoch_seconds(&self) -> f64;
    fn monotonic_seconds(&self) -> f64 {
        self.epoch_seconds()
    }
    fn local_date_time(&self) -> LocalDateTime;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn epoch_seconds(&self) -> f64 {
        system_epoch_seconds()
    }

    fn monotonic_seconds(&self) -> f64 {
        system_monotonic_seconds()
    }

    fn local_date_time(&self) -> LocalDateTime {
        let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
        LocalDateTime {
            year: now.year(),
            month: month_number(now.month()),
            day: now.day(),
            hour: now.hour(),
            minute: now.minute(),
            second: now.second(),
        }
    }
}

#[must_use]
pub(crate) fn system_epoch_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64())
}

#[must_use]
fn system_monotonic_seconds() -> f64 {
    fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|value| value.split_whitespace().next()?.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or_else(process_monotonic_seconds)
}

fn process_monotonic_seconds() -> f64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_secs_f64()
}

const fn month_number(month: Month) -> u8 {
    match month {
        Month::January => 1,
        Month::February => 2,
        Month::March => 3,
        Month::April => 4,
        Month::May => 5,
        Month::June => 6,
        Month::July => 7,
        Month::August => 8,
        Month::September => 9,
        Month::October => 10,
        Month::November => 11,
        Month::December => 12,
    }
}
