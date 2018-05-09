use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::result;

const EXT: &'static str = "data";
const SHARDING: &'static str = "SHARDING";
const PREFIX: &'static str = "/repo/flatfs/shard/";

#[derive(PartialEq, Debug)]
enum Shard {
	Suffix(u8),
	Prefix(u8),
	NextToLast(u8),
}

impl Shard {
	fn new(shard: &str, shard_length: u8) -> Self {
		use self::Shard::*;
		match shard {
			"next-to-last" => NextToLast(shard_length),
			// NOTE: I have not idea whether actually need to implement those kinds of shard.
			"suffix" => Suffix(shard_length),
			"prefix" => Prefix(shard_length),
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

#[derive(PartialEq, Debug)]
pub struct Datastore {
	path: PathBuf,
	shard: Shard,
}

#[derive(PartialEq, Debug)]
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
				Shard::new(
					shard_name,
					u8::from_str_radix(shard_length, 10)
						.expect(format!("shard_length [{}] can not parse as integer.", shard_length).as_ref()),
				)
			}
			Err(err) => unreachable!(err),
		}
	}

	pub fn new(path: String) -> Self {
		let path = Path::new(&path).to_path_buf();
		let mut path_of_shard_file = path.clone();
		path_of_shard_file.push(SHARDING);
		let sharding = match fs::read_dir(&path) {
			Ok(_) => match fs::File::open(&path_of_shard_file) {
				Ok(mut file) => {
					let mut buf = String::new();
					let _ = file.read_to_string(&mut buf);
					Some(buf)
				}
				Err(_) => None,
			},
			Err(_) => {
				let _ = fs::create_dir(&path);
				None
			}
		};
		let sharding = match sharding {
			Some(s) => s,
			None => {
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

#[cfg(test)]
mod test {
	use super::*;

	fn set_up(path: &str, shard: String) -> PathBuf {
		let tmp_dir = Path::new(&path).to_path_buf();
		let mut sharding = tmp_dir.clone();
		sharding.push(SHARDING);

		let _ = fs::create_dir(&tmp_dir);
		let _ = fs::File::create(&sharding).and_then(|mut file| file.write_all(shard.as_bytes()));
		tmp_dir
	}

	fn tear_down<P: AsRef<Path>>(path: P) {
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn test_new_create_dir_and_sharding() {
		let tmp_dir = ".tmp_dir-0";
		let x = Datastore::new(tmp_dir.to_owned());
		let dir = fs::read_dir(tmp_dir);
		assert!(dir.is_ok());
		assert_eq!(x.shard, Shard::NextToLast(2));

		tear_down(tmp_dir);
	}

	#[test]
	fn test_new_read_dir_when_exist() {
		let tmp_dir = set_up(".tmp_dir-1", format!("{}v2/{}/{}", PREFIX, "prefix", 100));
		let x = Datastore::new(tmp_dir.to_str().unwrap().to_owned());
		assert_eq!(x.shard, Shard::Prefix(100));
		tear_down(tmp_dir);
	}

	#[test]
	fn test_new_get() {
		let tmp_dir = set_up(
			".tmp_dir-2",
			format!("{}v1/{}/{}", PREFIX, "next-to-last", 2),
		);
		let mut sharding = tmp_dir.clone();
		sharding.push("AA");
		let _ = fs::create_dir(&sharding);
		sharding.push("BBBAAC.data");
		let _ = fs::File::create(&sharding).and_then(|mut file| {
			let sharding = format!("Test");
			file.write_all(sharding.as_bytes())
		});

		let x = Datastore::new(tmp_dir.to_str().unwrap().to_owned());
		let bytes = x.get("BBBAAC".to_owned()).unwrap();
		let actual = String::from_utf8(bytes).unwrap();
		assert_eq!(actual, "Test");
		tear_down(tmp_dir);
	}
}
