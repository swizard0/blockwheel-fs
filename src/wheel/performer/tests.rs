use super::{
    block,
    schema,
    super::storage,
    Performer,
};

fn init() -> Performer<()> {
    let storage_layout = storage::Layout {
        wheel_header_size: 24,
        block_header_size: 24,
        commit_tag_size: 16,
        eof_tag_size: 8,
    };
    let mut schema = schema::Schema::new(storage_layout);
    schema.initialize_empty(144);
    Performer::new(schema, 16)
}

fn sample_hello_world() -> block::Bytes {
    let mut block_bytes_mut = block::BytesMut::new();
    block_bytes_mut.extend("hello, world!".as_bytes().iter().cloned());
    block_bytes_mut.freeze()
}

#[test]
fn script_00() {
    let mut performer = init();

}
