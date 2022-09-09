use chrono::Duration;
use sqlx::postgres::types::PgInterval;

pub enum LimitedTimeFrame {
    Day1,
    Day30,
    Day7,
    Hour1,
    Minute5,
}

impl From<LimitedTimeFrame> for Duration {
    fn from(limited_time_frame: LimitedTimeFrame) -> Self {
        match limited_time_frame {
            LimitedTimeFrame::Day1 => Duration::days(1),
            LimitedTimeFrame::Day30 => Duration::days(30),
            LimitedTimeFrame::Day7 => Duration::days(7),
            LimitedTimeFrame::Hour1 => Duration::hours(1),
            LimitedTimeFrame::Minute5 => Duration::minutes(5),
        }
    }
}

impl From<LimitedTimeFrame> for PgInterval {
    fn from(limited_time_frame: LimitedTimeFrame) -> Self {
        PgInterval::try_from(Into::<Duration>::into(limited_time_frame)).unwrap()
    }
}

pub enum TimeFrame {
    All,
    LimitedTimeFrame(LimitedTimeFrame),
}

impl From<LimitedTimeFrame> for TimeFrame {
    fn from(limited_time_frame: LimitedTimeFrame) -> Self {
        TimeFrame::LimitedTimeFrame(limited_time_frame)
    }
}

impl TimeFrame {
    pub fn get_epoch_count(self) -> f64 {
        match self {
            TimeFrame::All => unimplemented!(),
            TimeFrame::LimitedTimeFrame(limited_time_frame) => match limited_time_frame {
                LimitedTimeFrame::Day1 => 225.0,
                LimitedTimeFrame::Day30 => 6750.0,
                LimitedTimeFrame::Day7 => 1575.0,
                LimitedTimeFrame::Hour1 => 9.375,
                LimitedTimeFrame::Minute5 => 0.78125,
            },
        }
    }
}
