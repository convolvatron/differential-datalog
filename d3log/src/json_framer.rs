use phf::{phf_map, phf_set};
use std::io::{Error, ErrorKind};

#[derive(Clone, Copy, PartialEq)]
#[allow(dead_code)]
enum Lexement {
    Object,
    Array,
    String,
}

#[allow(dead_code)]
static STARTS: phf::Map<char, Lexement> = phf_map! {
    '[' => Lexement::Array,
    '{' => Lexement::Object,
    '"' => Lexement::String,
};

#[allow(dead_code)]
static ENDS: phf::Map<char, Lexement> = phf_map! {
    ']'  => Lexement::Array ,
    '}'  => Lexement::Object,
};

#[allow(dead_code)]
static WHITESPACE: phf::Set<char> = phf_set! {' ', '\t', '\n'};

pub struct JsonFramer {
    w: Vec<Lexement>,
    reassembly: Vec<u8>,
}

impl JsonFramer {
    // certianly backslash - what else?
    //
    // we can avoid utf8 for the framing characters, but we currently dont handle
    // correct reassembly of utf8 across body boundaries
    //
    // dont really _like_ this interface, but ran into borrower problems, in any case
    // we want a stream, right?
    //
    pub fn append(&mut self, body: &[u8]) -> Result<Vec<String>, std::io::Error> {
        let mut n = Vec::new();
        for i in body {
            let c = &(*i as char);

            if match self.w.last() {
                Some(x) => *x == Lexement::String,
                None => false,
            } {
                self.reassembly.push(*i);
                if *c == '"' {
                    self.w.pop(); // backslash
                }
            } else {
                match STARTS.get(c) {
                    Some(x) => self.w.push(*x),
                    None => {
                        // dont allow atoms at the top level
                        if self.w.is_empty() && !WHITESPACE.contains(c) {
                            return Err(Error::new(
                                ErrorKind::Other,
                                format!(
                                    "extraneaous character {}",
                                    std::str::from_utf8(body).expect("")
                                ),
                            ));
                        }
                    }
                }

                if !self.w.is_empty() {
                    self.reassembly.push(*i);
                }

                if let Some(k) = ENDS.get(c) {
                    if *k != self.w.pop().expect("mismatched grouping") {
                        return Err(Error::new(ErrorKind::Other, "mismatched grouping"));
                    }
                    if self.w.is_empty() {
                        n.push(
                            std::str::from_utf8(&self.reassembly[..])
                                .map_err(|_| Error::new(ErrorKind::Other, "utf8 error"))?
                                .to_owned(),
                        );
                        self.reassembly.truncate(0);
                    }
                }
            }
        }
        Ok(n)
    }

    pub fn new() -> JsonFramer {
        JsonFramer {
            w: Vec::new(),
            reassembly: Vec::new(),
        }
    }
}