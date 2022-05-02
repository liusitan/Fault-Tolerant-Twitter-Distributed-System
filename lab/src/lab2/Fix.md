***Problems in lab2***
Lab 2 Issues

l2_test_clock_coarse_sync - [0/2] - clocks in different storage are coarsely synchronized
    Reason: 1 second?
    Status: TODO: ask Sitan for code of shutdown. Fixed the time interval

l2_test_concurrent_server_follows - [0/2] - your server handles concurrent follows
    Reason: should be one error one Ok
    Status: Fixed

l2_test_server_home_perf - [0/2] - home() on 1000 100-trib users returns in 7 seconds
    Reason: GC? TCP? TA said the getting all posts from all followers should be possible
    Status: TODO: merge code of connection reuse


l2_test_multi_bin_lists - [0/2] - verifies that you can use multiple bins on a single backend which gives distinct results on the KeyList trait // Qihang Zeng above test cases
    Status: TODO: do we still need to fix this in lab3?

l2_test_server_list_user_limit - [0/2] - list_user() returns less than 20 users when there are only 19 users signed up
    Status: TODO: Ask TA what is wrong

l2_test_server_tribble_causal - [0/2] - your tribble order reflects causal relationship
    Status: TODO: verify the fix. Fixed. 

l2_test_server_tribs_has_gc - [0/2] - Tribs() on a 5000-trib user does not read all 5000 tribs (has GC)lst
No gc trivisal

l2_test_multi_bin_values - [0/2] - you can use multiple bins on a single backend which give distinct results for methods in the KeyString trait // similar to KeyList one 

l2_test_server_follow_limit - [0/2] - follow() returns an error when following the 2001st user
    Status: TODO: verify the fix. Fixed.

l2_test_signals - [0/2] - ready/shutdown signals are handled properly
    Status: TODO: use the code of Sitan.

l2_test_store_multi_list_users - [0/2] - with 101 registered users, concurrent ListUsers()s from 100 users are dispatched to multiple back-ends
    Status: TODO: ask how should we implement this, and what is 
        "Keep multiple caches for the list_users() call when the users are many. Note that when the user count is more than 20, you don’t need to track new registered users anymore."