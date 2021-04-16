use warc::{WarcReader, Record};
use std::str;
use std::fs::OpenOptions;
use std::io::BufReader;
use flate2::read::MultiGzDecoder;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate anyhow;

use std::env;
use regex::Regex;

use scraper::{Html, Selector, ElementRef};

use immeta::load_from_buf;
use url::Url;
use httparse::Status::Complete;
use std::collections::HashMap;
use tfrecord::{Example, Feature, ExampleWriter, RecordWriterInit};

fn select_one<'a>(document: &'a Html, selector: &str) -> Result<ElementRef<'a>, anyhow::Error> {
    let selector = Selector::parse(selector).unwrap();
    let selected: Vec<_> = document.select(&selector).collect();

    if selected.len() != 1 {
        bail!("failed selecting {:?}, got {:?}", selector, selected)
    }

    return Ok(selected[0])
}

#[derive(Debug, Clone)]
struct ImageMeta {
    comment_count: u32,
    fave_count: u32,
    view_count: u32,

    license: String,

    tags: String,
    title: String,
    description: String,

    owner: String,
    img_src: String
}

#[derive(Debug, Clone)]
struct ImageExample {
    meta: ImageMeta,

    file: Vec<u8>,
    height: u32,
    width: u32,
}

fn int_feature(x: u32) -> Feature {
    Feature::Int64List(vec![x as i64])
}

fn string_feature(x: String) -> Feature {
    Feature::BytesList(vec![x.into_bytes()])
}

fn byte_feature(x: Vec<u8>) -> Feature {
    Feature::BytesList(vec![x])
}

impl ImageExample {
    fn into_example(self) -> Example {
        let mut example = HashMap::new();

        example.insert("comment_count".to_string(), int_feature(self.meta.comment_count));
        example.insert("fave_count".to_string(), int_feature(self.meta.fave_count));
        example.insert("view_count".to_string(), int_feature(self.meta.view_count));

        example.insert("height".to_string(), int_feature(self.height));
        example.insert("width".to_string(), int_feature(self.width));

        example.insert("license".to_string(), string_feature(self.meta.license));
        example.insert("tags".to_string(), string_feature(self.meta.tags));
        example.insert("title".to_string(), string_feature(self.meta.title));
        example.insert("description".to_string(), string_feature(self.meta.description));
        example.insert("owner".to_string(), string_feature(self.meta.owner));
        example.insert("img_src".to_string(), string_feature(self.meta.img_src));

        example.insert("image".to_string(), byte_feature(self.file));

        example
    }
}

fn is_canonical(url: &str) -> Result<bool, anyhow::Error> {
    let parsed = Url::parse(url)?;
    let filename = parsed.path_segments().unwrap().last().unwrap().to_string();

    lazy_static! {
        static ref RE: Regex = Regex::new(r"_[a-z]\.").unwrap();
    }

    Ok(!RE.is_match(&filename))
}

fn parse_meta(document: &Html, selector: &str) -> Option<String> {
    let element = select_one(&document, selector);
    match element {
        Ok(e) => {
            Some(e.value().attr("content").unwrap().to_string())
        }
        Err(_) => {
            None
        }
    }
}

// remove titles which are > 50% numbers
fn clean_title(title: &str) -> bool {
    // remove IMG, DCIM, DSC

    let title = title.to_lowercase().replace("img", "").replace("dcim", "").replace("dsc", "").replace("untitled", "");

    let numbers = title.chars().filter(|&x| '0' <= x && x <= '9').count();

    numbers * 2 < title.len()
}

fn parse_image_page(document: &Html) -> Result<ImageMeta, anyhow::Error> {
    let license = select_one(&document, ".photo-license-url")?.value().attr("href").unwrap();
    let comment_count: u32 = select_one(&document, ".comment-count-label")?.text().next().ok_or(anyhow!("comment count text not found"))?.trim().replace(",", "").parse()?;
    let fave_count: u32 = select_one(&document, ".fave-count-label")?.text().next().ok_or(anyhow!("fave count text not found"))?.trim().replace(",", "").parse()?;
    let view_count: u32 = select_one(&document, ".view-count-label")?.text().next().ok_or(anyhow!("view count text not found"))?.trim().replace(",", "").parse()?;

    let mut description = parse_meta(&document, "meta[property=\"og:description\"]").unwrap_or("".to_string());
    let mut title = parse_meta(&document, "meta[property=\"og:title\"]").unwrap_or("".to_string());

    let tags = parse_meta(&document, "meta[name=\"keywords\"]").unwrap_or("".to_string());

    if !clean_title(&title) {
        title.clear()
    }

    if description.ends_with(" photos to Flickr.") {
        description.clear()
    }

    let owner = select_one(&document, ".owner-name")?.text().next().unwrap();
    let img = select_one(&document, ".main-photo")?;
    let img_src = "https:".to_string() + img.value().attr("src").unwrap();

    Ok(ImageMeta {
        comment_count,
        fave_count,
        view_count,
        title,
        tags,
        description,
        license: license.to_string(),
        owner: owner.to_string(),
        img_src
    })
}

fn parse_record(record: &Record, all_meta: &mut HashMap<String, ImageMeta>) -> Result<Option<ImageExample>, anyhow::Error> {
    match &(record.headers["WARC-Type"])[..] {
        b"response" => {
            let target_uri = std::str::from_utf8(&record.headers["WARC-Target-URI"])?;

            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut resp = httparse::Response::new(&mut headers);

            let offset = match resp.parse(&record.body)? {
                Complete(offset) => offset,
                _ => bail!("incomplete HTTP request")
            };

            let http_body = &record.body[offset..];

            if target_uri.starts_with("https://www.flickr.com/photos/") {
                let body = std::str::from_utf8(&http_body)?;

                if !record.body.starts_with(b"HTTP/1.1 200") {
                    return Ok(None)
                }

                // lol we don't actually parse the HTTP, just feed it straight into the HTML parser
                let document = Html::parse_document(body);

                // adult content
                if select_one(&document, ".restricted-interstitial-message").is_ok() {
                    return Ok(None)
                }

                // weird map thingo
                if select_one(&document, "#f_div_osm_cc").is_ok() {
                    return Ok(None)
                }

                // all sizes list
                if select_one(&document, "#all-sizes-header").is_ok() {
                    return Ok(None)
                }

                let image_meta = parse_image_page(&document)?;
                // println!("imagemeta {:?}", image_meta);

                all_meta.insert(image_meta.img_src.clone(), image_meta);

                return Ok(None)
            } else if target_uri.contains("staticflickr.com") {
                if target_uri.contains("buddyicons") {
                    // println!("buddy {}", target_uri);
                } else {
                    let image_meta = load_from_buf(&http_body)?;
                    if is_canonical(target_uri)? {
                        // println!("canonical image {}, {:?}", target_uri, image_meta.dimensions());
                        if let Some(meta) = &all_meta.get(&target_uri.to_string()) {
                            return Ok(Some(ImageExample {
                                meta: (*meta).clone(),
                                file: http_body.to_vec(),
                                height: image_meta.dimensions().height,
                                width: image_meta.dimensions().width
                            }))
                        } else {
                            bail!("image not in meta")
                        }
                    } else {
                        // println!("image {}, {:?}", target_uri, meta.dimensions());
                    }
                }
            }
            else {
                // println!("uncategorized {}", target_uri);
            }
        }
        _ => {}
    }
    Ok(None)
}

fn main() -> Result<(), anyhow::Error> {
    let args: Vec<String> = env::args().collect();

    assert_eq!(args.len(), 3, "please call flickr_warc <input file> <output file>");

    let file = OpenOptions::new().read(true).open(&args[1])?;

    let gz = MultiGzDecoder::new(file);
    let text = BufReader::with_capacity(1024*1024*16, gz);
    let file = WarcReader::new(text);

    let outfile = OpenOptions::new().create(true).write(true).truncate(true).open(&args[2])?;
    let mut writer: ExampleWriter<_> = RecordWriterInit::from_writer(outfile)?;

    // let mut total = 0;
    // let mut image_count = 0;

    let mut all_meta = HashMap::new();

    for record in file {
        let record = record?;

        match parse_record(&record, &mut all_meta) {
            Ok(r) => {
                if let Some(meta) = r {
                    let example = meta.into_example();
                    writer.send(example)?;

                    // image_count += 1
                }
                // total += 1;
            },
            Err(_) => {
                // println!("writing file to err_{}", total);
                // let mut file = OpenOptions::new().create(true).write(true).open(&format!("dbg/err_{}", total))?;
                // file.write(&format!("{}", std::str::from_utf8(&record.headers["WARC-Target-URI"])?).into_bytes())?;
                // file.write(&record.body)?;
                // println!("error {}", e);
            }
        }
    }

    // println!("Total records: {}", total);
    // println!("Images: {}", image_count);

    Ok(())
}
