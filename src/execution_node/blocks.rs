pub struct BlockF {
    number: String,
}

pub struct Block {
    number: u32,
}

impl From<BlockF> for Block {
    fn from(f: BlockF) -> Self {
        Self {
            number: f.number.parse::<u32>().unwrap(),
        }
    }
}
