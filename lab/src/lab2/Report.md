***How the BinStorage works***

Every front-end will call new_bin_client() once to generate a BinStorage.

When created, a BinStorage puts given addresses into a list.

When bin(str) is called, a BinStorage hashes str, use this hash to choose an addr, and then return a storage using that addr

<ins>Note</ins>: I add "http://" to each addr, as mentioned in Piazza

***How the Tribbler works***

A Tribbler front-end will call new_bin_client() once, and call new_front() once. This generates a BinStorage, and then generates a Server that uses the BinStorage.

Here is how Server handles a call like signup("Alice"):

(1) b = bin("signup_log")

(2) use b.clock() to have an order

(3) b.list_append("list_signup", "Alice@@@" + order.string()) // "@@@" will be handled

(4) list = b.list_get("list_signup")

(5) 

    for entry in list
        if entry.name == "Alice" && entry.order > order
            // We are not the only one to signup Alice
            // Let's delete ourself from the list
            // So another one will signup Alice
            // Concurrent signups will both success
            b.list_remove("list_signup", "Alice@@@" + order.string())

<ins>Concurrency</ins>: Using the list_append of bin "signup_log", even if we don't have a lock server, we can guarantee that concurrent signup will be handled correctly. Same goes for Follow(), though it is a bit more complex.

***Job of Keeper***

Note that back-ends don't know Keeper but Keeper knows back-ends.

Keeper is a deadloop that for every two seconds, calls Clock() on every back-end, records the largest return value, and calls them again using the largest value (increment every time).

In this way, Keeper makes all back-ends' Clock() kinda synchronized.

***Note***

My implementation works well when using the website after `cargo run --bin trib-front -- -s lab`. Functionalities on the website are working as expected, including Tribble, Follow, Unfollow, Home, etc.

However, if launched with `cargo run --bin bins-client`, it appears that bins in the same backend are not isolated. This is because I didn't change my implementation of lab1's client and directly copied it to lab2's client. 

E.g., if bin("Alice") and bin("Bob") are in the same backend, execute these commands: `bin Alice`; `set a msg`; `bin Bob`; `get a`. You will see `Ok(Some("msg"))`.

I think this is fine, because in my Server's implementation I insolated different users. 

E.g., even if bin("Alice") and bin("Bob") are in the same backend, Alice's posts are in list "Alice::post_list" and Bob's posts are in list "Bob::post_list", so their posts are isolated.

***Credit***

Part of the code to handle shutdown of Keeper is from: https://users.rust-lang.org/t/wait-for-futures-in-loop/43007/3

