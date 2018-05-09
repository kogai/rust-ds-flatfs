use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::result;

const EXT: &'static str = "data";
const SHARDING: &'static str = "SHARDING";
const PREFIX: &'static str = "/repo/flatfs/shard/";

enum Shard {
	Suffix(u8),
	Prefix(u8),
	NextToLast(u8),
}

impl Shard {
	fn from_string(shard: &str, shard_length: u8) -> Self {
		use self::Shard::*;
		match shard {
			"suffix" => Suffix(shard_length),
			"prefix" => Prefix(shard_length),
			"next-to-last" => NextToLast(shard_length),
			_ => unreachable!("Invalid string [{}] has been passed.", shard),
		}
	}

	fn get_dir_name(&self, key: &String) -> String {
		use self::Shard::*;
		match self {
			Suffix(_shard_length) => key.chars().skip(1).collect::<String>(),

			Prefix(shard_length) => key.chars().take(*shard_length as usize).collect::<String>(),

			NextToLast(shard_length) => {
				let offset = (key.len() as u8) - shard_length - 1;
				key
					.chars()
					.skip(offset as usize)
					.take(*shard_length as usize)
					.collect::<String>()
			}
		}
	}
}

pub struct Datastore {
	path: PathBuf,
	shard: Shard,
}

pub struct Notfound(String);
pub type Result<T> = result::Result<T, Notfound>;

impl Datastore {
	fn parse_shard(sharding: String) -> Shard {
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
				Shard::from_string(
					shard_name,
					u8::from_str_radix(shard_length, 10)
						.expect(format!("shard_length [{}] can not parse as integer.", shard_length).as_ref()),
				)
			}
			Err(err) => unreachable!(err),
		}
	}

	pub fn new(path: String) -> Self {
		if let Ok(_) = fs::create_dir(&path) {};
		let path = Path::new(&path).to_path_buf();
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
		let shard = Datastore::parse_shard(sharding);
		Datastore { path, shard }
	}

	pub fn get(&self, key: String) -> Result<Vec<u8>> {
		let mut file = (&self).path.clone();
		file.push(self.shard.get_dir_name(&key));
		file.push(&key);
		let _ = file.set_extension(EXT);
		match fs::File::open(file) {
			Ok(mut file) => {
				let mut buf = Vec::new();
				let _ = file.read_to_end(&mut buf);
				Ok(buf)
			}
			Err(_) => Err(Notfound(format!("datastore: key [{}] not found.", key))),
		}
	}
}
