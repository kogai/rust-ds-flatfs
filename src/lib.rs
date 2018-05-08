use std::fs;
use std::io::{Read, Write};
use std::path::Path;

const SHARDING: &'static str = "SHARDING";
const PREFIX: &'static str = "/repo/flatfs/shard/";

enum Shard {
	Suffix,
	Prefix,
	NextToLast,
}

impl Shard {
	fn from_string(shard: &str) -> Self {
		use self::Shard::*;
		match shard {
			"suffix" => Suffix,
			"prefix" => Prefix,
			"next-to-last" => NextToLast,
			_ => unreachable!("Invalid string [{}] has been passed.", shard),
		}
	}
}

pub struct Datastore {
	path: String,
	shard: Shard,
	shard_length: u8,
}

impl Datastore {
	fn parse_shard(sharding: String) -> (Shard, u8) {
		let sharding = Path::new(&sharding);
		match sharding.strip_prefix(PREFIX) {
			Ok(rest_path) => {
				let mut params = rest_path
					.to_str()
					.expect(format!("Path {:?} can not convert to str", &rest_path).as_ref())
					.split("/");
				let _version = params.next().unwrap();
				let shard_name = params.next().unwrap();
				let shard_length = params.next().unwrap();
				(
					Shard::from_string(shard_name),
					u8::from_str_radix(shard_length, 10)
						.expect(format!("shard_length [{}] can not parse as integer.", shard_length).as_ref()),
				)
			}
			Err(err) => unreachable!(err),
		}
	}

	pub fn new(path: String) -> Self {
		if let Ok(_) = fs::create_dir(&path) {};
		let mut path_of_shard_file = Path::new(&path).to_path_buf();
		path_of_shard_file.push(SHARDING);
		let sharding = match fs::File::open(&path_of_shard_file) {
			Ok(mut file) => {
				let mut buf = String::new();
				let _ = file.read_to_string(&mut buf);
				buf
			}
			Err(_) => {
				// TODO: From default configuration
				let sharding = format!("{}v1/{}/{}", PREFIX, "next-to-last", 2);
				if let Ok(mut file) = fs::File::create(&path_of_shard_file) {
					let _ = file.write_all(sharding.as_bytes());
				};
				sharding
			}
		};
		let (shard, shard_length) = Datastore::parse_shard(sharding);
		Datastore {
			path,
			shard,
			shard_length,
		}
	}
}
