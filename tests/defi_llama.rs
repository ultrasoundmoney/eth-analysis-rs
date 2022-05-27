use eth_analysis::defi_llama::*;

#[test]
fn test_get_protocols() {
    let protocols = get_protocols();
    assert!(protocols.len() > 100)
}

#[test]
fn test_get_protocol() {
    let protocol = get_protocol("lido");
    assert_eq!(protocol.name, "Lido")
}
