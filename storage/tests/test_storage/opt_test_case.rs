pub enum TestCaseOption<P> {
    /// read test case from a json file
    ///     String: json file name in [xxx_test] folder
    FromFile(String),
    /// random generate test tuples
    /// use P parameter
    GenParam(P),
}
