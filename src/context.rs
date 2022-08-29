pub trait Context {
    type Info;
    type Flush;
    type WriteBlock;
    type ReadBlock;
    type DeleteBlock;
    type IterBlocks;
    type IterBlocksStream;
}
