//! The doc string of a transaction, read as `etcdctl txn` reads it: up to
//! three sections separated by an empty line (compares, the operations on
//! success, the operations on failure), one compare or operation per line.
//!
//! Every error names the line of the doc string (the first line is 1) and
//! never quotes what it read: at dispatch the text has been interpolated, and
//! a value may be a secret.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cmp {
    Equal,
    NotEqual,
    Less,
    Greater,
}

/// What a compare reads of the key, and what it is compared with: text for
/// `value`, a number for the rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Test {
    Value(String),
    Version(i64),
    Create(i64),
    Mod(i64),
    Lease(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compare {
    pub key: String,
    pub cmp: Cmp,
    pub test: Test,
}

/// Where a `get` or a `del` reads: one key, a range up to `range_end`, or
/// every key under the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Span {
    Key(String),
    Range(String, String),
    Prefix(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Put {
        key: String,
        value: String,
        lease: Option<i64>,
    },
    Get(Span),
    Del(Span),
}

impl Span {
    pub fn key(&self) -> &str {
        match self {
            Self::Key(key) | Self::Range(key, _) | Self::Prefix(key) => key,
        }
    }
}

impl Operation {
    pub fn key(&self) -> &str {
        match self {
            Self::Put { key, .. } => key,
            Self::Get(span) | Self::Del(span) => span.key(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Parsed {
    pub compares: Vec<Compare>,
    pub success: Vec<Operation>,
    pub failure: Vec<Operation>,
}

use crate::steps::EMPTY_PREFIX;

fn at(line: usize, message: &str) -> String {
    format!("line {line}: {message}")
}

/// `shape` is the check before the first request: a number that holds a
/// `<<slot>>` is accepted as it is and read again once it has a value.
struct Reader {
    shape: bool,
}

/// Parses the text of a doc string, one leading and one trailing line break
/// already removed (`ops::doc_value`).
pub fn parse(text: &str, shape: bool) -> Result<Parsed, String> {
    let reader = Reader { shape };
    let mut sections: Vec<Vec<(usize, &str)>> = vec![Vec::new()];
    let mut fourth = None;
    for (index, line) in text.trim_end().split('\n').enumerate() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.trim().is_empty() {
            sections.push(Vec::new());
            if sections.len() == 4 {
                fourth = Some(index + 2);
            }
        } else if let Some(section) = sections.last_mut() {
            section.push((index + 1, line));
        }
    }
    if let Some(line) = fourth {
        return Err(at(
            line,
            "a fourth section: a transaction has the compares, the operations on success and the operations on failure",
        ));
    }
    sections.resize(3, Vec::new());
    let mut parsed = Parsed::default();
    for &(line, text) in &sections[0] {
        parsed.compares.push(reader.compare(line, text)?);
    }
    for &(line, text) in &sections[1] {
        parsed.success.push(reader.operation(line, text)?);
    }
    for &(line, text) in &sections[2] {
        parsed.failure.push(reader.operation(line, text)?);
    }
    Ok(parsed)
}

/// One token: its text, and whether it was in double quotes (a quoted
/// `--prefix` is a key, not a flag).
struct Token {
    text: String,
    quoted: bool,
}

struct Cursor<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    line: usize,
}

impl Cursor<'_> {
    fn spaces(&mut self) {
        while self.chars.next_if(|c| *c == ' ' || *c == '\t').is_some() {}
    }

    /// The text between double quotes, `\"` and `\\` unescaped.
    fn quoted(&mut self) -> Result<String, String> {
        if self.chars.next() != Some('"') {
            return Err(at(self.line, "a double quote was expected"));
        }
        let mut out = String::new();
        loop {
            match self.chars.next() {
                Some('"') => return Ok(out),
                Some('\\') => match self.chars.next() {
                    Some(c @ ('"' | '\\')) => out.push(c),
                    _ => return Err(at(self.line, "only \\\" and \\\\ may follow a backslash")),
                },
                Some(c) => out.push(c),
                None => return Err(at(self.line, "a double quote is not closed")),
            }
        }
    }

    fn word(&mut self, stop: impl Fn(char) -> bool) -> String {
        let mut out = String::new();
        while let Some(c) = self.chars.next_if(|c| !stop(*c)) {
            out.push(c);
        }
        out
    }

    fn ended(&mut self) -> bool {
        self.spaces();
        self.chars.peek().is_none()
    }
}

fn cursor(text: &str, line: usize) -> Cursor<'_> {
    Cursor {
        chars: text.chars().peekable(),
        line,
    }
}

fn tokens(line: usize, text: &str) -> Result<Vec<Token>, String> {
    let mut cursor = cursor(text, line);
    let mut out = Vec::new();
    while !cursor.ended() {
        let quoted = cursor.chars.peek() == Some(&'"');
        let text = if quoted {
            cursor.quoted()?
        } else {
            cursor.word(|c| c == ' ' || c == '\t' || c == '"')
        };
        if matches!(cursor.chars.peek(), Some(c) if *c != ' ' && *c != '\t') {
            return Err(at(
                line,
                "a double quote inside a word: put the whole token in double quotes",
            ));
        }
        out.push(Token { text, quoted });
    }
    Ok(out)
}

impl Reader {
    /// A decimal number, or (for the shape check) a token with a slot.
    fn number(&self, line: usize, text: &str, what: &str) -> Result<i64, String> {
        if self.shape && text.contains("<<") {
            return Ok(0);
        }
        text.parse::<u64>()
            .ok()
            .and_then(|n| i64::try_from(n).ok())
            .ok_or_else(|| at(line, &format!("{what} is not a decimal number")))
    }

    fn compare(&self, line: usize, source: &str) -> Result<Compare, String> {
        let mut cursor = cursor(source, line);
        cursor.spaces();
        let target = cursor.word(|c| c == '(' || c == ' ' || c == '\t');
        let number: Option<fn(i64) -> Test> = match target.as_str() {
            "value" | "val" => None,
            "version" | "ver" => Some(Test::Version),
            "create" | "c" => Some(Test::Create),
            "mod" | "m" => Some(Test::Mod),
            "lease" | "l" => Some(Test::Lease),
            _ => {
                return Err(at(
                    line,
                    "not a compare: it reads <target>(\"<key>\") <op> \"<value>\", the targets are value, version, create, mod and lease",
                ));
            }
        };
        if cursor.chars.next() != Some('(') {
            return Err(at(line, "a compare names its key as <target>(\"<key>\")"));
        }
        let key = cursor.quoted()?;
        if cursor.chars.next() != Some(')') {
            return Err(at(line, "a closing parenthesis was expected after the key"));
        }
        cursor.spaces();
        let cmp = match cursor.word(|c| c == ' ' || c == '\t' || c == '"').as_str() {
            "=" => Cmp::Equal,
            "!=" => Cmp::NotEqual,
            "<" => Cmp::Less,
            ">" => Cmp::Greater,
            _ => return Err(at(line, "the operator of a compare is =, !=, < or >")),
        };
        cursor.spaces();
        let value = cursor.quoted()?;
        if !cursor.ended() {
            return Err(at(line, "nothing may follow the value of a compare"));
        }
        let test = match number {
            None => Test::Value(value),
            Some(test) => test(self.number(line, &value, "the value of this compare")?),
        };
        Ok(Compare { key, cmp, test })
    }

    fn operation(&self, line: usize, text: &str) -> Result<Operation, String> {
        let tokens = tokens(line, text)?;
        let Some((first, rest)) = tokens.split_first() else {
            return Err(at(line, "an operation was expected"));
        };
        let mut positional: Vec<&str> = Vec::new();
        let (mut lease, mut prefix) = (None, false);
        for token in rest {
            match (token.quoted, token.text.as_str()) {
                (false, "--prefix") => {
                    if std::mem::replace(&mut prefix, true) {
                        return Err(at(line, "--prefix is given twice"));
                    }
                }
                (false, flag) if flag.starts_with("--") => {
                    let Some(id) = flag.strip_prefix("--lease=") else {
                        return Err(at(
                            line,
                            "unknown flag: put takes --lease=<id>, get and del take --prefix",
                        ));
                    };
                    if lease.is_some() {
                        return Err(at(line, "--lease is given twice"));
                    }
                    lease = Some(self.number(line, id, "the lease ID")?);
                }
                (_, word) => positional.push(word),
            }
        }
        let own = |s: &str| s.to_string();
        let span = |name: &str| -> Result<Span, String> {
            if lease.is_some() {
                return Err(at(line, "--lease belongs to put"));
            }
            match (positional.as_slice(), prefix) {
                ([key], false) => Ok(Span::Key(own(key))),
                // A prefix of nothing is the whole keyspace, and a delete of it
                // is `I delete all etcd keys`, never a slip.
                ([""], true) if name == "del" => Err(at(line, EMPTY_PREFIX)),
                ([key], true) => Ok(Span::Prefix(own(key))),
                ([key, end], false) => Ok(Span::Range(own(key), own(end))),
                ([_, _], true) => Err(at(
                    line,
                    "a range end and --prefix cannot be given together",
                )),
                _ => Err(at(
                    line,
                    &format!("{name} takes a key, and optionally a range end"),
                )),
            }
        };
        match (first.quoted, first.text.as_str()) {
            (false, "put") => {
                if prefix {
                    return Err(at(line, "--prefix does not belong to put"));
                }
                let [key, value] = positional.as_slice() else {
                    return Err(at(line, "put takes a key and a value"));
                };
                Ok(Operation::Put {
                    key: own(key),
                    value: own(value),
                    lease,
                })
            }
            (false, "get") => span("get").map(Operation::Get),
            (false, "del") => span("del").map(Operation::Del),
            _ => Err(at(line, "an operation is put, get or del")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(text: &str) -> Parsed {
        parse(text, false).expect("parsed")
    }

    fn err(text: &str) -> String {
        parse(text, false).expect_err("refused")
    }

    #[test]
    fn three_sections_are_the_compares_the_success_and_the_failure() {
        let parsed = ok("create(\"/lock\") = \"0\"\n\nput /lock me\n\nget /lock");
        assert_eq!(parsed.compares.len(), 1);
        assert_eq!(parsed.success.len(), 1);
        assert_eq!(
            parsed.failure,
            vec![Operation::Get(Span::Key("/lock".into()))]
        );
    }

    #[test]
    fn blank_lines_at_the_end_are_no_section() {
        let parsed = ok("c(\"a\") = \"0\"\n\nput a 1\n\nget a\n\n");
        assert_eq!(parsed.failure.len(), 1);
    }

    #[test]
    fn a_delete_of_an_empty_prefix_is_refused_and_a_get_is_not() {
        let error = err("\ndel \"\" --prefix");
        assert!(
            error.starts_with("line 2:") && error.contains("I delete all etcd keys"),
            "{error}"
        );
        assert!(parse("\ndel \"\" --prefix", true).is_err());
        assert!(parse("\nget \"\" --prefix", false).is_ok());
        assert!(parse("\ndel a --prefix", false).is_ok());
    }

    #[test]
    fn a_missing_section_is_empty() {
        assert_eq!(ok(""), Parsed::default());
        let only_compares = ok("version(\"/k\") > \"1\"");
        assert_eq!(
            (only_compares.success.len(), only_compares.failure.len()),
            (0, 0)
        );
        // An empty compares section is an empty first one.
        let unconditional = ok("\nput /k v");
        assert!(unconditional.compares.is_empty());
        assert_eq!(unconditional.success.len(), 1);
        // An empty success section sits between two empty lines.
        let failure_only = ok("value(\"/k\") = \"a\"\n\n\nput /k b");
        assert!(failure_only.success.is_empty());
        assert_eq!(failure_only.failure.len(), 1);
    }

    #[test]
    fn a_fourth_section_is_an_error_naming_its_line() {
        assert_eq!(
            err("c(\"a\") = \"0\"\n\nput a 1\n\nget a\n\nget b")[..7].to_string(),
            "line 7:"
        );
        // An empty fourth section counts too.
        assert!(err("a\n\nb\n\nc\n\n\n\nd").starts_with("line 7:"));
    }

    #[test]
    fn every_target_and_operator_has_its_names() {
        let test = |name: &str| {
            ok(&format!("{name}(\"/k\") = \"1\"")).compares[0]
                .test
                .clone()
        };
        for name in ["version", "ver"] {
            assert_eq!(test(name), Test::Version(1), "{name}");
        }
        for name in ["create", "c"] {
            assert_eq!(test(name), Test::Create(1), "{name}");
        }
        for name in ["mod", "m"] {
            assert_eq!(test(name), Test::Mod(1), "{name}");
        }
        for name in ["lease", "l"] {
            assert_eq!(test(name), Test::Lease(1), "{name}");
        }
        for name in ["value", "val"] {
            assert_eq!(test(name), Test::Value("1".into()), "{name}");
        }
        for (op, want) in [
            ("=", Cmp::Equal),
            ("!=", Cmp::NotEqual),
            ("<", Cmp::Less),
            (">", Cmp::Greater),
        ] {
            let line = format!("mod(\"/k\") {op} \"1\"");
            assert_eq!(ok(&line).compares[0].cmp, want, "{op}");
        }
    }

    #[test]
    fn a_value_compare_keeps_text_and_the_others_need_a_decimal_number() {
        assert_eq!(
            ok("value(\"/k\") = \"a b\"").compares[0].test,
            Test::Value("a b".into())
        );
        assert_eq!(ok("mod(\"/k\") < \"12\"").compares[0].test, Test::Mod(12));
        for bad in ["x", "-1", "0x1", "", "1.5", "99999999999999999999"] {
            let error = err(&format!("version(\"/k\") = \"{bad}\""));
            assert!(
                error.starts_with("line 1:") && error.contains("decimal"),
                "{error}"
            );
        }
    }

    #[test]
    fn compares_have_one_shape() {
        for bad in [
            "key(\"/k\") = \"1\"",
            "value \"/k\" = \"1\"",
            "value(/k) = \"1\"",
            "value(\"/k\" = \"1\"",
            "value(\"/k\") == \"1\"",
            "value(\"/k\") = 1",
            "value(\"/k\") = \"1\" extra",
        ] {
            assert!(err(bad).starts_with("line 1:"), "{bad}");
        }
    }

    #[test]
    fn quoting_takes_spaces_and_escapes() {
        let parsed = ok(
            "value(\"/a \\\"b\\\"\") = \"x\\\\y\"\n\nput \"/my key\" \"a \\\"b\\\" \\\\ c\" --lease=7",
        );
        assert_eq!(parsed.compares[0].key, "/a \"b\"");
        assert_eq!(parsed.compares[0].test, Test::Value("x\\y".into()));
        assert_eq!(
            parsed.success,
            vec![Operation::Put {
                key: "/my key".into(),
                value: "a \"b\" \\ c".into(),
                lease: Some(7)
            }]
        );
        assert!(err("\nput a \"open").starts_with("line 2:"));
        assert!(err("\nput a \"\\n\"").starts_with("line 2:"));
        assert!(err("\nput a b\"c\"").starts_with("line 2:"));
        assert!(err("\nput a \"b\"c").starts_with("line 2:"));
        // An empty value is a value.
        assert!(
            matches!(&ok("\nput a \"\"").success[0], Operation::Put { value, .. } if value.is_empty())
        );
    }

    #[test]
    fn the_operations_and_their_flags() {
        let parsed =
            ok("\nget a\nget a b\nget a --prefix\ndel a\ndel a b\ndel a --prefix\nput a b");
        assert_eq!(
            parsed.success[..6],
            [
                Operation::Get(Span::Key("a".into())),
                Operation::Get(Span::Range("a".into(), "b".into())),
                Operation::Get(Span::Prefix("a".into())),
                Operation::Del(Span::Key("a".into())),
                Operation::Del(Span::Range("a".into(), "b".into())),
                Operation::Del(Span::Prefix("a".into())),
            ]
        );
        // A quoted flag is a key.
        assert_eq!(
            ok("\ndel \"--prefix\"").success,
            vec![Operation::Del(Span::Key("--prefix".into()))]
        );
    }

    #[test]
    fn what_is_outside_the_syntax_is_an_error_with_its_line() {
        for (text, line) in [
            ("\nget a b --prefix", 2),
            ("\nget a --keys-only", 2),
            ("\nget a --lease=1", 2),
            ("\nput a b --prefix", 2),
            ("\nput a b --lease=x", 2),
            ("\nput a b --lease=1 --lease=2", 2),
            ("\nput a", 2),
            ("\nput a b c", 2),
            ("\nget", 2),
            ("\nget a b c", 2),
            ("\ntxn a", 2),
            ("\nput a b\nincr a", 3),
            ("\"put\" a b", 1),
            ("c(\"a\") = \"0\"\n\nput a b\nget a --foo\n\nget a --foo", 4),
        ] {
            let error = err(text);
            assert!(
                error.starts_with(&format!("line {line}:")),
                "{text}: {error}"
            );
        }
    }

    #[test]
    fn a_compare_may_be_indented_and_tab_separated() {
        let parsed = ok("  value(\"/k\")\t=\t\"1\"");
        assert_eq!(parsed.compares[0].test, Test::Value("1".into()));
    }

    #[test]
    fn a_slot_passes_the_shape_check_and_is_read_again_with_its_value() {
        let text = "mod(\"/k\") = \"<<rev>>\"\n\nput a b --lease=<<id>>";
        assert!(parse(text, true).is_ok());
        assert!(parse(text, false).is_err());
        // A slot does not excuse a wrong flag.
        assert!(parse("\nput a b --lease=<<id>> --prefix", true).is_err());
    }

    #[test]
    fn an_error_never_quotes_what_it_read() {
        for bad in [
            "\nput a --s3cret-flag",
            "\nzap s3cret",
            "mod(\"s3cret\") = \"s3cret\"",
        ] {
            assert!(!err(bad).contains("s3cret"), "{bad}");
        }
    }
}
