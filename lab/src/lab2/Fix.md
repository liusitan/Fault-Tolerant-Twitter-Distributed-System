***Problems in lab2***
Lab 2 Issues

l2_test_clock_coarse_sync - [0/2] - clocks in different storage are coarsely synchronized
    Reason: 1 second?
    Status: TODO4: test with TA. Fixed

l2_test_concurrent_server_follows - [0/2] - your server handles concurrent follows
    Reason: should be one error one Ok
    Status: Fixed

l2_test_server_home_perf - [0/2] - home() on 1000 100-trib users returns in 7 seconds
    Reason: GC? TCP? TA said the getting all posts from all followers should be possible
    Status: TODO1: verify with TA. Fixed.


l2_test_multi_bin_lists - [0/2] - verifies that you can use multiple bins on a single backend which gives distinct results on the KeyList trait // Qihang Zeng above test cases
    Status: TODO3: verify with TA. Fixed.

l2_test_server_list_user_limit - [0/2] - list_user() returns less than 20 users when there are only 19 users signed up
    Status: Fixed.

l2_test_server_tribble_causal - [0/2] - your tribble order reflects causal relationship
    Status: TODO2: verify with TA. Fixed. 


l2_test_server_tribs_has_gc - [0/2] - Tribs() on a 5000-trib user does not read all 5000 tribs (has GC)lst
    Status: TODO: do GC of tribs

l2_test_multi_bin_values - [0/2] - you can use multiple bins on a single backend which give distinct results for methods in the KeyString trait // similar to KeyList one 

l2_test_server_follow_limit - [0/2] - follow() returns an error when following the 2001st user
    Status: Fixed.

l2_test_signals - [0/2] - ready/shutdown signals are handled properly
    Status: Fixed.

l2_test_store_multi_list_users - [0/2] - with 101 registered users, concurrent ListUsers()s from 100 users are dispatched to multiple back-ends
    Status: TODO: put the global user list into multiple bins.