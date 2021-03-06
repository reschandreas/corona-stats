use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike, Utc};
use csv::{ReaderBuilder, StringRecord};
use serde::de;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;

const URL_DAILY_REPORT: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/";
const URL_TIME_SERIES: &str = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-";

//https://stackoverflow.com/questions/57614558/how-to-use-custom-serde-deserializer-for-chrono-timestamps
struct NaiveDateTimeVisitor;

impl<'de> de::Visitor<'de> for NaiveDateTimeVisitor {
    type Value = NaiveDateTime;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a string represents chrono::NaiveDateTime")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
            Ok(t) => Ok(t),
            Err(_) => Err(de::Error::invalid_value(de::Unexpected::Str(s), &self)),
        }
    }
}

fn from_timestamp<'de, D>(d: D) -> Result<NaiveDateTime, D::Error>
where
    D: de::Deserializer<'de>,
{
    d.deserialize_str(NaiveDateTimeVisitor)
}

struct CsvRecord {
    province: String,
    country: String,
    updated: String,
    confirmed: u32,
    deaths: u32,
    recovered: u32,
    lat: Option<f32>,
    long: Option<f32>,
}

#[derive(Debug, Deserialize, Clone)]
struct Record {
    province: String,
    country: String,
    #[serde(deserialize_with = "from_timestamp")]
    updated: NaiveDateTime,
    confirmed: u32,
    deaths: u32,
    recovered: u32,
    lat: Option<f32>,
    long: Option<f32>,
}

#[derive(Debug, Clone)]
struct TimeSeries {
    province: String,
    country: String,
    lat: Option<f32>,
    long: Option<f32>,
    data: BTreeMap<String, i32>,
    state: String,
}

pub fn get_data() -> Result<(), Box<dyn Error>> {
    let mut map = HashMap::new();

    for elem in get_dates().iter() {
        for e in get_data_from(elem)?.iter() {
            let entry = map.entry(e.country.clone()).or_insert(Vec::new());
            entry.push(e.clone());
        }
    }
    println!("{:?}", map);
    Ok(())
}

pub fn get_series() -> Result<(), Box<dyn Error>> {
    for elem in get_time_series()?.iter() {
        if elem.country == "Italy" {
            println!("{:?}", elem.country);
            for d in elem.data.iter() {
                println!("{:?}", d);
            }
            //println!("{:?}", elem);
        }    
    }
    Ok(())
}

#[tokio::main]
async fn get_data_from(date: &NaiveDate) -> Result<Vec<Record>, Box<dyn Error>> {
    let mut data = Vec::new();
    let url = format!("{}{}.csv", URL_DAILY_REPORT, date.format("%m-%d-%Y"));
    
    let body = reqwest::get(&url).await?.text().await?;

    let mut rdr = ReaderBuilder::new()
        .delimiter(b',')
        .from_reader(body.as_bytes());

    for result in rdr.records() {
        let row: Record = to_record(normalize(result?));
        data.push(row);
    }
    Ok(data)
}

fn normalize(record: StringRecord) -> CsvRecord {
    CsvRecord {
        province: match record.get(0) {
            Some(t) => t.to_string(),
            None => "".to_string(),
        },
        country: match record.get(1) {
            Some(t) => t.to_string(),
            None => "".to_string(),
        },
        updated: match record.get(2) {
            Some(t) => t.to_string(),
            None => "".to_string(),
        },
        confirmed: match record.get(3) {
            Some(t) => match t.to_string().parse::<u32>() {
                Ok(t) => t,
                Err(_) => 0,
            },
            None => 0,
        },
        deaths: match record.get(4) {
            Some(t) => match t.to_string().parse::<u32>() {
                Ok(t) => t,
                Err(_) => 0,
            },
            None => 0,
        },
        recovered: match record.get(5) {
            Some(t) => match t.to_string().parse::<u32>() {
                Ok(t) => t,
                Err(_) => 0,
            },
            None => 0,
        },
        lat: match record.get(6) {
            Some(t) => match t.to_string().parse::<f32>() {
                Ok(t) => Some(t),
                Err(_) => None::<f32>,
            },
            None => None::<f32>,
        },
        long: match record.get(7) {
            Some(t) => match t.to_string().parse::<f32>() {
                Ok(t) => Some(t),
                Err(_) => None::<f32>,
            },
            None => None::<f32>,
        },
    }
}

fn to_record(record: CsvRecord) -> Record {
    Record {
        province: record.province,
        country: record.country,
        updated: parse_date(record.updated),
        confirmed: record.confirmed,
        deaths: record.deaths,
        recovered: record.recovered,
        lat: record.lat,
        long: record.long,
    }
}

fn parse_date(s: String) -> NaiveDateTime {
    for format in [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d%y %H:%M",
        "%m/%d/%Y %H:%M",
    ]
    .iter()
    {
        match NaiveDateTime::parse_from_str(&s, format) {
            Ok(t) => {
                if t.year() < 2000 {
                    return NaiveDate::from_ymd(t.year() + 2000, t.month(), t.day()).and_hms(
                        t.hour(),
                        t.minute(),
                        t.second(),
                    );
                } else {
                    return t;
                }
            }
            Err(_) => (),
        }
    }
    NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0)
}

fn get_dates() -> Vec<NaiveDate> {
    let mut dates = Vec::new();
    let mut date = NaiveDate::from_ymd(2020, 1, 22);
    let now = Utc::now();
    let mut now = NaiveDate::from_ymd(now.year(), now.month(), now.day());
    now = now.succ();

    while date != now {
        dates.push(date);
        date = date.succ();
    }

    dates
}

#[tokio::main]
async fn get_time_series() -> Result<Vec<TimeSeries>, Box<dyn Error>> {
    let mut series = Vec::new();

    for state in ["Confirmed", "Deaths", "Recovered"].iter() {
        let url = format!("{}{}.csv", URL_TIME_SERIES, state);
        
        let body = reqwest::get(&url).await?.text().await?;

        let mut rdr = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(body.as_bytes());

        for rlt in rdr.records() {
            let result: StringRecord = rlt?;
            let mut record = TimeSeries {
                province: match result.get(0) {
                    Some(t) => t.to_string(),
                    None => "".to_string(),
                },
                country: match result.get(1) {
                    Some(t) => t.to_string(),
                    None => "".to_string(),
                },
                lat: match result.get(2) {
                    Some(t) => match t.to_string().parse::<f32>() {
                        Ok(t) => Some(t),
                        Err(_) => None::<f32>,
                    },
                    None => None::<f32>,
                },
                long: match result.get(3) {
                    Some(t) => match t.to_string().parse::<f32>() {
                        Ok(t) => Some(t),
                        Err(_) => None::<f32>,
                    },
                    None => None::<f32>,
                },
                data: BTreeMap::new(),
                state: state.to_string(),
            };
            let mut index = 4;
            let mut date = NaiveDate::from_ymd(2020, 1, 22);
            loop {
                record.data.insert(date.to_string(), match result.get(index) {
                    Some(t) => match t.to_string().parse::<i32>() {
                        Ok(t) => t,
                        Err(_) => -1,
                    },
                    None => break,
                });
                if *record.data.get(&date.to_string()).unwrap() < 0 {
                    record.data.remove(&date.to_string());
                }
                index += 1;
                date = date.succ();
            }
            series.push(record);
        }
    }

    Ok(series)
}
