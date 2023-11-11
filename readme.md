# oh no

## basic types

```rust
type User {
  id: int,
  firstname: string,
  lastname: string
}
```

```sql
select * from user where id = 1;
```

## sum types

```rust
type Color = 
  RGB {
    red: int,
    green: int,
    blue: int
  } | 
  Greyscale {
    greyscale: int
  }
```

Select via constructor to ignore other constructors

```sql
select RGB{red,green} from color; # red: int, green: int
select Greyscale{greyscale} from color; # greyscale: int
```

Some sort of eventual pattern matching syntax feels inevitable.

## dealing with overlapping columns

For ergonomics, we probably want to be able to select across constructors in a
lens-like fashion too.

```rust
type MultiUser =
  InternalUser { id: int, name: string } |
  ExternalUser { id: int, email: string }
```

```sql
select id from multiuser; # id: int
select email from multiuser; # email: Maybe<string> --? not sure about this so
much
```


