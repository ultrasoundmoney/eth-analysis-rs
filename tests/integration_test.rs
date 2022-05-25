use eth_analysis::defi_llama;

#[test]
fn get_protocols_from_defi_llama() {
    let protocols = defi_llama::get_protocols();
    assert!(protocols.len() > 100)
}

#[test]
fn get_protocol() {
    let protocol = defi_llama::get_protocol("uniswap");
    assert_eq!(protocol.name, "Uniswap")
}
