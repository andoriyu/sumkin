#[macro_use]
extern crate lazy_static;

pub mod traits;
pub mod error;
pub mod sqlite;
pub mod log;

pub type Revision = i64;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
