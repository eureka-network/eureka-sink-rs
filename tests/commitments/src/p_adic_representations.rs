use ethereum_types::U256;
use plonky2::field::goldilocks_field::GoldilocksField;
use plonky2::field::types::{Field, Field64, PrimeField64};

use crate::{Digest, EventsCommitment, F};

pub(crate) fn goldilocks_adic_representation(x: U256) -> [F; 5] {
    // 5 field elements are sufficient to represent a U256 number
    let mut result = [F::ZERO; 5];
    let mut value = x;
    for i in 0..result.len() {
        let (q, r) = value.div_mod(F::ORDER.into());
        // we know that remainder < ORDER < MAXu64
        result[i] = F::from_canonical_u64(r.low_u64());
        value = q;
    }
    result
}

pub(crate) fn from_adic_representation(x: [F; 5]) -> U256 {
    let mut result: U256 = U256::from(x[4].to_canonical_u64());
    for i in 0..4 {
        result = result * U256::from(F::ORDER) + U256::from(x[3 - i].to_canonical_u64());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex::FromHex;
    use sha256::digest;

    #[test]
    fn it_works_goldilocks_adic_repr() {
        println!("our digest: {}", digest("hello world"));
        let x = U256::from_little_endian(&Vec::<u8>::from_hex(digest("hello world")).unwrap());
        println!("x = {}", x);
        let y = goldilocks_adic_representation(x);
        let z = from_adic_representation(y);
        assert_eq!(x, z);
    }
}
