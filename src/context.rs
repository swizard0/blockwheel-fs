use std::fmt::Debug;

pub trait Context {
    type Info;
    type LendBlock: Debug;
    type WriteBlock: Debug;
    type ReadBlock: Debug;
    type DeleteBlock: Debug;
    type Interpreter;
}
