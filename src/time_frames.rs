use std::{fmt::Display, str::FromStr};

use chrono::{DateTime, Duration, Utc};
use enum_iterator::Sequence;
use serde::{Serialize, Serializer};
use sqlx::postgres::types::PgInterval;
use thiserror::Error;

use crate::execution_chain::{
    self, BlockNumber, ExecutionNodeBlock, LONDON_HARD_FORK_BLOCK_NUMBER, MERGE_BLOCK_NUMBER,
};

use GrowingTimeFrame::*;
use LimitedTimeFrame::*;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Sequence)]
pub enum LimitedTimeFrame {
    Day1,
    Day30,
    Day7,
    Hour1,
    Minute5,
}

impl LimitedTimeFrame {
    pub fn epoch_count(self) -> f64 {
        match self {
            Day1 => 225.0,
            Day30 => 6750.0,
            Day7 => 1575.0,
            Hour1 => 9.375,
            Minute5 => 0.78125,
        }
    }

    pub fn slot_count(self) -> u32 {
        match self {
            Day1 => 7200,
            Day30 => 216000,
            Day7 => 50400,
            Hour1 => 300,
            Minute5 => 25,
        }
    }

    pub fn postgres_interval(&self) -> PgInterval {
        match self {
            Day1 => PgInterval {
                months: 0,
                days: 1,
                microseconds: 0,
            },
            Day30 => PgInterval {
                months: 0,
                days: 30,
                microseconds: 0,
            },
            Day7 => PgInterval {
                months: 0,
                days: 7,
                microseconds: 0,
            },
            Hour1 => PgInterval {
                months: 0,
                days: 0,
                microseconds: Duration::hours(1).num_microseconds().unwrap(),
            },
            Minute5 => PgInterval {
                months: 0,
                days: 0,
                microseconds: Duration::minutes(5).num_microseconds().unwrap(),
            },
        }
    }

    pub fn to_db_key(self) -> &'static str {
        match self {
            Day1 => "d1",
            Day30 => "d1",
            Day7 => "d7",
            Hour1 => "h1",
            Minute5 => "m5",
        }
    }

    pub fn duration(&self) -> Duration {
        self.into()
    }
}

impl From<&LimitedTimeFrame> for Duration {
    fn from(limited_time_frame: &LimitedTimeFrame) -> Self {
        match limited_time_frame {
            Day1 => Duration::days(1),
            Day30 => Duration::days(30),
            Day7 => Duration::days(7),
            Hour1 => Duration::hours(1),
            Minute5 => Duration::minutes(5),
        }
    }
}

impl From<&LimitedTimeFrame> for PgInterval {
    fn from(limited_time_frame: &LimitedTimeFrame) -> Self {
        PgInterval::try_from(Into::<Duration>::into(limited_time_frame)).unwrap()
    }
}

#[derive(Debug, Error)]
pub enum ParseTimeFrameError {
    #[error("failed to parse time frame {0}")]
    UnknownTimeFrame(String),
}

impl FromStr for LimitedTimeFrame {
    type Err = ParseTimeFrameError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "m5" => Ok(Minute5),
            "h1" => Ok(Hour1),
            "d1" => Ok(Day1),
            "d7" => Ok(Day7),
            "d30" => Ok(Day30),
            unknown_time_frame => Err(ParseTimeFrameError::UnknownTimeFrame(
                unknown_time_frame.to_string(),
            )),
        }
    }
}

impl Display for LimitedTimeFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use LimitedTimeFrame::*;
        match self {
            Day1 => write!(f, "d1"),
            Day30 => write!(f, "d30"),
            Day7 => write!(f, "d7"),
            Hour1 => write!(f, "h1"),
            Minute5 => write!(f, "m5"),
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Sequence)]
pub enum GrowingTimeFrame {
    SinceBurn,
    SinceMerge,
}

impl GrowingTimeFrame {
    pub fn start_timestamp(&self) -> DateTime<Utc> {
        match self {
            SinceBurn => *execution_chain::LONDON_HARD_FORK_TIMESTAMP,
            SinceMerge => *execution_chain::PARIS_HARD_FORK_TIMESTAMP,
        }
    }

    pub fn start_block_number(&self) -> BlockNumber {
        match self {
            SinceBurn => LONDON_HARD_FORK_BLOCK_NUMBER,
            SinceMerge => MERGE_BLOCK_NUMBER,
        }
    }

    pub fn duration(&self) -> Duration {
        self.into()
    }
}

impl From<&GrowingTimeFrame> for Duration {
    fn from(growing_time_frame: &GrowingTimeFrame) -> Self {
        Utc::now() - growing_time_frame.start_timestamp()
    }
}

impl Display for GrowingTimeFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use GrowingTimeFrame::*;
        match self {
            SinceBurn => write!(f, "since_burn"),
            SinceMerge => write!(f, "since_merge"),
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Sequence)]
pub enum TimeFrame {
    Growing(GrowingTimeFrame),
    Limited(LimitedTimeFrame),
}

const MINUTES_PER_HOUR: f64 = 60.0;
const HOURS_PER_DAY: f64 = 24.0;
const DAYS_PER_YEAR: f64 = 365.25;

impl TimeFrame {
    pub fn duration(&self) -> Duration {
        match self {
            TimeFrame::Growing(growing_time_frame) => growing_time_frame.duration(),
            TimeFrame::Limited(limited_time_frame) => limited_time_frame.duration(),
        }
    }

    pub fn years_f64(&self) -> f64 {
        // Duration units are rounded. The smallest is 5 minutes, therefore num_minutes().
        self.duration().num_minutes() as f64 / MINUTES_PER_HOUR / HOURS_PER_DAY / DAYS_PER_YEAR
    }

    pub fn start_timestamp(&self, block: &ExecutionNodeBlock) -> DateTime<Utc> {
        match self {
            TimeFrame::Growing(growing_time_frame) => growing_time_frame.start_timestamp(),
            TimeFrame::Limited(limited_time_frame) => {
                block.timestamp - limited_time_frame.duration()
            }
        }
    }
}

impl From<LimitedTimeFrame> for TimeFrame {
    fn from(limited_time_frame: LimitedTimeFrame) -> Self {
        TimeFrame::Limited(limited_time_frame)
    }
}

impl FromStr for TimeFrame {
    type Err = ParseTimeFrameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use GrowingTimeFrame::*;
        use TimeFrame::*;

        match s {
            "all" => Ok(Growing(SinceBurn)),
            "since_burn" => Ok(Growing(SinceBurn)),
            "since_merge" => Ok(Growing(SinceMerge)),
            unknown_time_frame => unknown_time_frame.parse().map(Limited),
        }
    }
}

impl Display for TimeFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TimeFrame::*;
        match self {
            Growing(growing_time_frame) => write!(f, "{growing_time_frame}"),
            Limited(limited_time_frame) => write!(f, "{limited_time_frame}"),
        }
    }
}

impl Serialize for TimeFrame {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use enum_iterator::all;

    use super::*;

    #[test]
    fn time_frame_iter_test() {
        let time_frames = all::<TimeFrame>().collect::<Vec<_>>();
        let expected = vec![
            TimeFrame::Growing(SinceBurn),
            TimeFrame::Growing(SinceMerge),
            TimeFrame::Limited(Day1),
            TimeFrame::Limited(Day30),
            TimeFrame::Limited(Day7),
            TimeFrame::Limited(Hour1),
            TimeFrame::Limited(Minute5),
        ];

        assert_eq!(expected, time_frames);
    }

    #[test]
    fn parse_test() {
        let time_frame = "all".parse::<TimeFrame>().unwrap();
        assert_eq!(time_frame, TimeFrame::Growing(SinceBurn));

        let limited_time_frame = "d30".parse::<TimeFrame>().unwrap();
        assert_eq!(limited_time_frame, TimeFrame::Limited(Day30))
    }
}
