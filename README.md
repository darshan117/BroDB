# BroDB:

## What's the Deal, Bro? 

Welcome to BroDB, the database that's got your back like a true bro! Written from scratch in Go (because real bros don't need no third-party libraries).

## Features 😎

- **B-tree Indexing**: Our B-trees are so jacked, they make binary trees look like twigs.
- **Table Rendering**: Tables so pretty, you'll want to frame them and hang them in your man cave.
- **Bro Query Language (BQL)**: SQL's cooler, more laid-back cousin. It's like talking to your bros, but for data!

## Getting Started: Time to Crush It! 

1. Clone this repo 
2. Build it 
3. Run it (./brodb dbfile_name)

## Bro Query Language: Speak Bro, Query Bro 

### Create a Table 

```
BRO, LET'S BUILD THIS PLAYBOOK pickup_lines (id INT,the_line TEXT,success_rate INT);

# Set the primary_key using

BRO, LET'S BUILD THIS PLAYBOOK pickup_lines (id INT,the_line TEXT,success_rate INT) id PRIMARY_KEY;

```

### Insert Data

```
 BRO, SLAM THIS INTO pickup_line (id,the_line) THIS CRAZY SHIT (7,"Sup, hottie?");
```

### Select Data 

```
BRO, SHOW ME ALL FROM pickup_lines;

# Or get specific, bro
BRO, SHOW ME (id , the_line) FROM pickup_lines WHERE id = 7 and the_line="Sup, hottie?";
```

### Delete Data 

```
BRO, DITCH THIS CRAP FROM pickup_lines where id =7;
```

- **Bro-Friendly**: If your database doesn't talk like your bros, is it even a real database?

## Contributing: Become a Bro-veloper 

1. Fork it (like you fork your bicep curls)
2. Create your feature branch (`git checkout -b feature/AmazingBroFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingBroFeature'`)
4. Push to the branch (`git push origin feature/AmazingBroFeature`)
5. Open a Pull Request (and flex on them code reviewers)

---

