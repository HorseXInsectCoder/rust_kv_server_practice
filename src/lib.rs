mod service;
mod pb;
mod errors;
mod storage;


pub use pb::{*, abi::*};
pub use service::*;
pub use storage::*;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
