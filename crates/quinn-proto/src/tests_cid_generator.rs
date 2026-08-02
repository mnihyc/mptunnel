use super::*;

#[test]
fn validate_keyed_cid() {
    let mut generator = HashedConnectionIdGenerator::new();
    let cid = generator.generate_cid();
    generator.validate(&cid).unwrap();
}
