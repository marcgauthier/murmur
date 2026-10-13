//! Inspect the owned public Rust surface without requiring nightly rustdoc JSON.
use std::{collections::BTreeSet, path::Path};
use syn::{Item, UseTree, Visibility};
fn public(v: &Visibility) -> bool {
    matches!(v, Visibility::Public(_))
}
fn names(tree: &UseTree, out: &mut BTreeSet<String>) {
    match tree {
        UseTree::Path(p) => names(&p.tree, out),
        UseTree::Name(n) => {
            out.insert(n.ident.to_string());
        }
        UseTree::Rename(n) => {
            out.insert(n.rename.to_string());
        }
        UseTree::Group(g) => {
            for t in &g.items {
                names(t, out)
            }
        }
        UseTree::Glob(_) => panic!("public glob needs an explicit inventory rule"),
    }
}
fn impl_name(i: &syn::ItemImpl) -> Option<String> {
    if let syn::Type::Path(p) = i.self_ty.as_ref() {
        p.path.segments.last().map(|s| s.ident.to_string())
    } else {
        None
    }
}
fn scan(path: &Path, prefix: &str, allowed: Option<&BTreeSet<String>>, out: &mut BTreeSet<String>) {
    let file = syn::parse_file(&std::fs::read_to_string(path).unwrap()).unwrap();
    let accept = |name: &str| allowed.is_none_or(|a| a.contains(name));
    for item in file.items {
        match item {
            Item::Use(u) if public(&u.vis) => {
                let mut exported = BTreeSet::new();
                names(&u.tree, &mut exported);
                for name in exported {
                    out.insert(format!("{prefix}{name}"));
                }
            }
            Item::Mod(m) if public(&m.vis) => {
                out.insert(format!("{prefix}{}", m.ident));
            }
            Item::Struct(s) if public(&s.vis) && accept(&s.ident.to_string()) => {
                let name = format!("{prefix}{}", s.ident);
                out.insert(name.clone());
                for f in s.fields {
                    if public(&f.vis) {
                        if let Some(id) = f.ident {
                            out.insert(format!("{name}.{id}"));
                        }
                    }
                }
            }
            Item::Enum(e) if public(&e.vis) && accept(&e.ident.to_string()) => {
                let name = format!("{prefix}{}", e.ident);
                out.insert(name.clone());
                for v in e.variants {
                    out.insert(format!("{name}::{}", v.ident));
                }
            }
            Item::Fn(f) if public(&f.vis) && accept(&f.sig.ident.to_string()) => {
                out.insert(format!("{prefix}{}", f.sig.ident));
            }
            Item::Const(c) if public(&c.vis) && accept(&c.ident.to_string()) => {
                out.insert(format!("{prefix}{}", c.ident));
            }
            Item::Type(t) if public(&t.vis) && accept(&t.ident.to_string()) => {
                out.insert(format!("{prefix}{}", t.ident));
            }
            Item::Trait(t) if public(&t.vis) && accept(&t.ident.to_string()) => {
                let name = format!("{prefix}{}", t.ident);
                out.insert(name.clone());
                for item in t.items {
                    if let syn::TraitItem::Fn(f) = item {
                        out.insert(format!("{name}::{}", f.sig.ident));
                    }
                }
            }
            Item::Impl(i) => {
                if let Some(name) = impl_name(&i) {
                    if accept(&name) {
                        for item in i.items {
                            match item {
                                syn::ImplItem::Fn(f) if public(&f.vis) => {
                                    out.insert(format!("{prefix}{name}::{}", f.sig.ident));
                                }
                                syn::ImplItem::Const(c) if public(&c.vis) => {
                                    out.insert(format!("{prefix}{name}::{}", c.ident));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}
fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut out = BTreeSet::new();
    let lib = syn::parse_file(&std::fs::read_to_string(root.join("src/lib.rs")).unwrap()).unwrap();
    let mut exported = BTreeSet::new();
    let mut modules = Vec::new();
    for i in &lib.items {
        match i {
            Item::Use(u) if public(&u.vis) => names(&u.tree, &mut exported),
            Item::Mod(m) => modules.push((m.ident.to_string(), public(&m.vis))),
            _ => {}
        }
    }
    scan(&root.join("src/lib.rs"), "sqlite_session::", None, &mut out);
    for (name, is_public) in modules {
        let path = root.join(format!("src/{name}.rs"));
        if path.exists() {
            let public_prefix = format!("sqlite_session::{name}::");
            scan(
                &path,
                if is_public {
                    &public_prefix
                } else {
                    "sqlite_session::"
                },
                if is_public { None } else { Some(&exported) },
                &mut out,
            );
        }
    }
    let lib =
        syn::parse_file(&std::fs::read_to_string(root.join("replication/src/lib.rs")).unwrap())
            .unwrap();
    let mut exports = BTreeSet::new();
    for i in &lib.items {
        if let Item::Use(u) = i {
            if public(&u.vis) {
                names(&u.tree, &mut exports)
            }
        }
    }
    scan(
        &root.join("replication/src/lib.rs"),
        "replication::",
        None,
        &mut out,
    );
    scan(
        &root.join("replication/src/engine.rs"),
        "replication::",
        Some(&exports),
        &mut out,
    );
    for module in lib.items.iter().filter_map(|i| {
        if let Item::Mod(m) = i {
            if public(&m.vis) {
                return Some(m.ident.to_string());
            }
        }
        None
    }) {
        scan(
            &root.join(format!("replication/src/{module}.rs")),
            &format!("replication::{module}::"),
            None,
            &mut out,
        );
    }
    let keys = BTreeSet::from([
        "DataKeyInfo".into(),
        "KeyInventory".into(),
        "KeyStatus".into(),
    ]);
    scan(
        &root.join("spool/src/keymgmt.rs"),
        "sqlite_session::",
        Some(&keys),
        &mut out,
    );
    scan(
        &root.join("spool/src/keyring.rs"),
        "sqlite_session::",
        Some(&keys),
        &mut out,
    );
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
}
