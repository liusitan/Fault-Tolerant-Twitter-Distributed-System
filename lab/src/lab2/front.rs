use async_trait::async_trait;
use std::cmp::Ordering;
use std::sync::Arc;
use std::time::SystemTime;
use tribbler::{
    err::TribResult, err::TribblerError, storage, storage::BinStorage, trib::is_valid_username,
    trib::Server, trib::Trib, trib::MAX_FOLLOWING, trib::MAX_TRIB_FETCH, trib::MAX_TRIB_LEN,
    trib::MIN_LIST_USER,
};

pub struct FrontServer {
    pub bin_storage: Box<dyn BinStorage>,
}

#[derive(Debug)]
pub struct StringAndTime {
    pub string: String,
    pub time: u64,
}

impl StringAndTime {
    pub fn to_str(&self) -> String {
        let s = [self.string.clone(), self.time.to_string().clone()].join("@@@");
        return s;
    }
    pub fn from_str(s: &str) -> StringAndTime {
        let st = s.to_string();
        let index = match st.find("@@@") {
            Some(i) => i,
            None => 0, // this is impossible anyway
        };
        let user = st[0..index].to_string();
        let time: u64 = match st[index + 3..].to_string().parse() {
            Ok(t) => t,
            Err(_) => 0, // this is impossible anyway
        };
        return StringAndTime {
            string: user,
            time: time,
        };
    }
}

#[derive(Debug, Clone)]
pub struct FollowEntry {
    pub whom: String,
    pub is_follow: bool,
    pub time: u64,
}

impl FollowEntry {
    pub fn to_str(&self) -> String {
        let s = [self.whom.clone(), self.is_follow.to_string().clone()].join("@@@");
        let s2 = [s, self.time.to_string().clone()].join("&&&");
        return s2;
    }
    pub fn from_str(s: &str) -> FollowEntry {
        let st = s.to_string();
        let index_is_f = match st.find("@@@") {
            Some(i) => i,
            None => 0, // this is impossible anyway
        };
        let whom = st[0..index_is_f].to_string();

        let index_time = match st.find("&&&") {
            Some(i) => i,
            None => 0, // this is impossible anyway
        };
        let is_follow: bool = match st[index_is_f + 3..index_time].to_string().parse() {
            Ok(t) => t,
            Err(_) => true, // this is impossible anyway
        };
        let time: u64 = match st[index_time + 3..].to_string().parse() {
            Ok(t) => t,
            Err(_) => 0, // this is impossible anyway
        };

        return FollowEntry {
            whom: whom,
            is_follow: is_follow,
            time: time,
        };
    }
}

#[derive(Debug)]
pub struct TribInfo {
    pub logical_time: u64,
    pub physical_time: u64,
    pub user: String,
    pub post: String,
}

impl TribInfo {
    pub fn to_str(&self) -> String {
        let s = [self.post.clone(), self.user.clone()].join("###");
        let s2 = [s, self.logical_time.to_string().clone()].join("@@@");
        let s3 = [s2, self.physical_time.to_string().clone()].join("&&&");
        return s3;
    }
    pub fn from_str(s: &str) -> TribInfo {
        let st = s.to_string();
        let index_user = match st.find("###") {
            Some(i) => i,
            None => 0, // this is impossible anyway
        };
        let post = st[0..index_user].to_string();
        let index_logical = match st.find("@@@") {
            Some(i) => i,
            None => 0, // this is impossible anyway
        };
        let user = st[index_user + 3..index_logical].to_string();

        let index_physical = match st.find("&&&") {
            Some(i) => i,
            None => 0, // this is impossible anyway
        };

        let logical_t: u64 = match st[index_logical + 3..index_physical].to_string().parse() {
            Ok(t) => t,
            Err(_) => 0, // this is impossible anyway
        };

        let physical_t: u64 = match st[index_physical + 3..].to_string().parse() {
            Ok(t) => t,
            Err(_) => 0, // this is impossible anyway
        };
        return TribInfo {
            post: post,
            user: user,
            logical_time: logical_t,
            physical_time: physical_t,
        };
    }
}

impl Ord for TribInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        // (
        //     other.logical_time,
        //     other.physical_time,
        //     &other.user,
        //     &other.post,
        // )
        //     .cmp(&(
        //         self.logical_time,
        //         self.physical_time,
        //         &self.user,
        //         &self.post,
        //     ))
        (
            self.logical_time,
            self.physical_time,
            &self.user,
            &self.post,
        )
            .cmp(&(
                other.logical_time,
                other.physical_time,
                &other.user,
                &other.post,
            ))
    }
}

impl PartialOrd for TribInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TribInfo {
    fn eq(&self, other: &Self) -> bool {
        (
            self.logical_time,
            self.physical_time,
            &self.user,
            &self.post,
        ) == (
            other.logical_time,
            other.physical_time,
            &other.user,
            &other.post,
        )
    }
}

impl Eq for TribInfo {}

/*
Structure of bins in backends:
(1) bin name: SIGNUP_LOG
    (1.1) list SIGNUP_LOG::list // contains strings from UserAndTime.to_str(), like "Alice@10"
(2) bin for user. E.g.: Alice
    (2.1) list Alice::post_list // contains strings of posts
    (2.2) list Alice::follow_list // contains strings from FollowEntry.to_str(), like "Bob@true" and "Bob@false", which means first follow Bob and then unfollow him

*/

// TODO: remove unsuccess entries in SIGNUP_LOG::list
// TODO: remove old posts in Alice::post
// TODO: remove redundant entries in Alice::follow

pub const SIGNUP_LOG: &str = "SIGNUP_LOG";
pub const SIGNUP_LOG_LIST: &str = "SIGNUP_LOG::list";
pub const POST_LIST: &str = "post_list";
pub const FOLLOW_LIST: &str = "follow_list";

pub fn post_list(user: String) -> String {
    let s = [user, POST_LIST.to_string()].join("::");
    return s;
}

pub fn follow_list(user: String) -> String {
    let s = [user, FOLLOW_LIST.to_string()].join("::");
    return s;
}

#[async_trait]
impl Server for FrontServer {
    /// Creates a user.
    /// Returns error when the username is invalid;
    /// returns error when the user already exists.
    /// Concurrent sign ups on the same user might both succeed with no error.
    async fn sign_up(&self, user: &str) -> TribResult<()> {
        // check invalid username
        if is_valid_username(user) == false {
            let e =
                TribblerError::InvalidUsername("sign_up: invalid user name:".to_string() + user);
            return Err(Box::new(e));
        }
        // check already existing user
        let bin_signup_log = self.bin_storage.bin(SIGNUP_LOG).await?;
        let list = bin_signup_log.list_get(SIGNUP_LOG_LIST).await?;
        for entry in list.0 {
            let entry_u_and_t = StringAndTime::from_str(&entry);
            if entry_u_and_t.string == user {
                let e = TribblerError::UsernameTaken(user.to_string());
                return Err(Box::new(e));
            }
        }

        // pack the username with a time
        let time = bin_signup_log.clock(0).await?;
        let u_and_t = StringAndTime {
            string: user.to_string(),
            time: time,
        };
        let u_and_t_string = u_and_t.to_str();

        // store this to SIGNUP_LOG's list SIGNUP_LOG_LIST
        let k_v = storage::KeyValue::new(SIGNUP_LOG_LIST, &u_and_t_string);
        let _r = bin_signup_log.list_append(&k_v).await?;

        // get the list back
        let mut flag_found = false;
        let list = bin_signup_log.list_get(SIGNUP_LOG_LIST).await?;
        for entry in list.0 {
            let entry_u_and_t = StringAndTime::from_str(&entry);
            if entry_u_and_t.string == user {
                // see if there is another one who has larger timestamp than us
                if entry_u_and_t.time == time {
                    // found ourself
                    flag_found = true;
                } else if entry_u_and_t.time > time {
                    // we are not the only one, delete ourself
                    let _r = bin_signup_log.list_remove(&k_v).await?;
                    return Ok(());
                }
            }
        }
        if flag_found {
            return Ok(());
        } else {
            let e = TribblerError::Unknown(
                "sign_up: User missing after append and get:".to_string() + user,
            );
            return Err(Box::new(e));
        }
    }

    /// List 20 registered users.  When there are less than 20 users that
    /// signed up the service, all of them needs to be listed.  When there
    /// are more than 20 users that signed up the service, an arbitrary set
    /// of at lest 20 of them needs to be listed.
    /// The result should be sorted in alphabetical order.
    async fn list_users(&self) -> TribResult<Vec<String>> {
        let mut user_list: Vec<String> = Vec::new();
        let bin_signup_log = self.bin_storage.bin(SIGNUP_LOG).await?;
        let list = bin_signup_log.list_get(SIGNUP_LOG_LIST).await?;
        for entry in list.0 {
            let entry_u_and_t = StringAndTime::from_str(&entry);
            user_list.push(entry_u_and_t.string.clone());
            if user_list.len() == MIN_LIST_USER {
                break;
            }
        }
        user_list.sort();
        return Ok(user_list);
    }

    /// Post a tribble.  The clock is the maximum clock value this user has
    /// seen so far by reading tribbles or clock sync. // QUESTION: how should I use this clock
    /// Returns error when who does not exist;
    /// returns error when post is too long.
    async fn post(&self, who: &str, post: &str, clock: u64) -> TribResult<()> {
        // check if post is longer than MAX_TRIB_LEN
        if post.to_string().len() > MAX_TRIB_LEN {
            let e = TribblerError::TribTooLong;
            return Err(Box::new(e));
        }
        // check if not existing user
        let bin_signup_log = self.bin_storage.bin(SIGNUP_LOG).await?;
        let list = bin_signup_log.list_get(SIGNUP_LOG_LIST).await?;
        let mut flag_exist = false;
        for entry in list.0 {
            let entry_u_and_t = StringAndTime::from_str(&entry);
            if entry_u_and_t.string == who.to_string() {
                flag_exist = true;
                break;
            }
        }
        if flag_exist == false {
            let e = TribblerError::InvalidUsername(who.to_string());
            return Err(Box::new(e));
        }

        // puts the post into who::POST_LIST
        let bin_user = self.bin_storage.bin(who).await?;
        let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => n.as_micros(),
            Err(_) => {
                let e = TribblerError::Unknown("post: error during using SystemTime".to_string());
                return Err(Box::new(e));
            }
        };
        let now_u64 = (now - 1650700000000000) as u64; // make the time smaller
        let p_and_t = TribInfo {
            post: post.to_string(),
            user: who.to_string(),
            physical_time: now_u64,
            logical_time: clock,
        };
        let p_and_t_string = p_and_t.to_str();
        let k_v = storage::KeyValue::new(&post_list(who.to_string()), &p_and_t_string);
        let _r = bin_user.list_append(&k_v).await?;

        return Ok(());
    }

    /// List the tribs that a particular user posted.
    /// Returns error when user has not signed up.
    async fn tribs(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        // check if not existing user
        let bin_signup_log = self.bin_storage.bin(SIGNUP_LOG).await?;
        let list = bin_signup_log.list_get(SIGNUP_LOG_LIST).await?;
        let mut flag_exist = false;
        for entry in list.0 {
            let entry_u_and_t = StringAndTime::from_str(&entry);
            if entry_u_and_t.string == user.to_string() {
                flag_exist = true;
                break;
            }
        }
        if flag_exist == false {
            let e = TribblerError::InvalidUsername(user.to_string());
            return Err(Box::new(e));
        }

        // find all the posts
        let bin_user = self.bin_storage.bin(user).await?;

        // add call to clock() before we get the posts
        let _ = bin_user.clock(0).await?;

        let list = bin_user.list_get(&post_list(user.to_string())).await?;
        // parse all the tribs and sort them
        let mut trib_list_all: Vec<TribInfo> = Vec::new();
        for entry in list.0 {
            let entry_trib_info = TribInfo::from_str(&entry);
            trib_list_all.push(entry_trib_info);
        }
        trib_list_all.sort();

        // return the most recent MAX_TRIB_FETCH posts
        let mut trib_list_ret: Vec<Arc<Trib>> = Vec::new();
        for entry in trib_list_all {
            if trib_list_ret.len() == MAX_TRIB_FETCH {
                break;
            }
            let t = Trib {
                user: user.to_string(),
                message: entry.post,
                clock: entry.logical_time,
                time: entry.physical_time,
            };
            let n = Arc::new(t);
            trib_list_ret.push(n);
        }
        return Ok(trib_list_ret);
    }

    /// Follow someone's timeline.
    /// Returns error when who == whom;
    /// returns error when who is already following whom;
    /// returns error when who is trying to following
    /// more than trib.MaxFollowing users.
    /// returns error when who or whom has not signed up.
    /// Concurrent follows might both succeed without error.
    /// The count of following users might exceed trib.MaxFollowing=2000,
    /// if and only if the 2000'th user is generated by concurrent Follow()
    /// calls.
    async fn follow(&self, who: &str, whom: &str) -> TribResult<()> {
        // check if who == whom
        if who.to_string() == whom.to_string() {
            let e = TribblerError::Unknown("follow: follow one's self:".to_string() + who);
            return Err(Box::new(e));
        }
        // check if not existing user
        let bin_signup_log = self.bin_storage.bin(SIGNUP_LOG).await?;
        let list = bin_signup_log.list_get(SIGNUP_LOG_LIST).await?;
        let (mut flag_exist1, mut flag_exist2) = (false, false);
        for entry in list.0 {
            let entry_u_and_t = StringAndTime::from_str(&entry);
            if entry_u_and_t.string == who.to_string() {
                flag_exist1 = true;
            }
            if entry_u_and_t.string == whom.to_string() {
                flag_exist2 = true;
            }
        }
        if flag_exist1 == false {
            let e = TribblerError::InvalidUsername(who.to_string());
            return Err(Box::new(e));
        }
        if flag_exist2 == false {
            let e = TribblerError::InvalidUsername(whom.to_string());
            return Err(Box::new(e));
        }

        // check if already following or maxfollowing
        let bin_user = self.bin_storage.bin(who).await?;
        let list = bin_user.list_get(&follow_list(who.to_string())).await?;
        let mut follow_count = 0;
        for entry in list.0 {
            let entry_f = FollowEntry::from_str(&entry);
            if entry_f.whom == whom && entry_f.is_follow {
                let e = TribblerError::AlreadyFollowing(who.to_string(), whom.to_string());
                return Err(Box::new(e));
            }
            if entry_f.is_follow {
                follow_count += 1;
            }
        }
        if follow_count >= MAX_FOLLOWING {
            let e = TribblerError::FollowingTooMany;
            return Err(Box::new(e));
        }

        // pack the follow entrywith a time
        let time = bin_user.clock(0).await?;
        let e = FollowEntry {
            whom: whom.to_string(),
            time: time,
            is_follow: true,
        };
        let e_string = e.to_str();

        // store this to who's follow list
        let k_v = storage::KeyValue::new(&follow_list(who.to_string()), &e_string);
        let _r = bin_user.list_append(&k_v).await?;

        // get the list back
        let mut flag_found = false;
        let list = bin_user.list_get(&follow_list(who.to_string())).await?;
        for entry in list.0 {
            let entry_f = FollowEntry::from_str(&entry);
            if entry_f.whom == whom.to_string() && entry_f.is_follow {
                // see if there is another one who has larger timestamp than us
                if entry_f.time == time {
                    // found ourself
                    flag_found = true;
                } else if entry_f.time > time {
                    // delete our entry and return OK
                    let _r = bin_user.list_remove(&k_v).await?;
                    let e = TribblerError::AlreadyFollowing(who.to_string(), whom.to_string());
                    return Err(Box::new(e));
                }
            }
        }
        if flag_found == false {
            let e =
                TribblerError::Unknown("follow: Entry missing after append and get:".to_string());
            return Err(Box::new(e));
        }

        return Ok(());
    }

    /// Unfollow someone's timeline.
    /// Returns error when who == whom.
    /// returns error when who is not following whom;
    /// returns error when who or whom has not signed up.
    async fn unfollow(&self, who: &str, whom: &str) -> TribResult<()> {
        // check if who == whom
        if who.to_string() == whom.to_string() {
            let e = TribblerError::Unknown("follow: follow one's self:".to_string() + who);
            return Err(Box::new(e));
        }

        // check if not existing user
        let bin_signup_log = self.bin_storage.bin(SIGNUP_LOG).await?;
        let list = bin_signup_log.list_get(SIGNUP_LOG_LIST).await?;
        let (mut flag_exist1, mut flag_exist2) = (false, false);
        for entry in list.0 {
            let entry_u_and_t = StringAndTime::from_str(&entry);
            if entry_u_and_t.string == who.to_string() {
                flag_exist1 = true;
            }
            if entry_u_and_t.string == whom.to_string() {
                flag_exist2 = true;
            }
        }
        if flag_exist1 == false {
            let e = TribblerError::InvalidUsername(who.to_string());
            return Err(Box::new(e));
        }
        if flag_exist2 == false {
            let e = TribblerError::InvalidUsername(whom.to_string());
            return Err(Box::new(e));
        }

        // check if not following
        let bin_user = self.bin_storage.bin(who).await?;
        let list = bin_user.list_get(&follow_list(who.to_string())).await?;
        let mut flag_found = false;
        let mut follow_entry = FollowEntry {
            whom: "unknownUser".to_string(),
            is_follow: true,
            time: 0,
        };
        for entry in list.0 {
            let entry_f = FollowEntry::from_str(&entry);
            if entry_f.whom == whom && entry_f.is_follow {
                flag_found = true;
                follow_entry = entry_f.clone();
            }
        }
        if flag_found == false {
            let e = TribblerError::NotFollowing(who.to_string(), whom.to_string());
            return Err(Box::new(e));
        }

        // pack the follow entrywith a time
        let time = bin_user.clock(0).await?;
        let e = FollowEntry {
            whom: whom.to_string(),
            time: time,
            is_follow: false,
        };
        let e_string = e.to_str();

        // store this to who's follow list
        let k_v = storage::KeyValue::new(&follow_list(who.to_string()), &e_string);
        let _r = bin_user.list_append(&k_v).await?;

        // get the list back
        let mut flag_found = false;
        let list = bin_user.list_get(&follow_list(who.to_string())).await?;
        for entry in list.0 {
            let entry_f = FollowEntry::from_str(&entry);
            if entry_f.whom == whom.to_string() && entry_f.is_follow == false {
                // see if there is another one who has larger timestamp than us
                if entry_f.time == time {
                    // found ourself
                    flag_found = true;
                } else if entry_f.time > time {
                    // delete our entry and return OK
                    let _r = bin_user.list_remove(&k_v).await?;
                    return Ok(());
                }
            }
        }
        if flag_found == false {
            let e =
                TribblerError::Unknown("follow: Entry missing after append and get:".to_string());
            return Err(Box::new(e));
        }

        // remove the previous follow entry and remove ourself
        let k_v_prev =
            storage::KeyValue::new(&follow_list(who.to_string()), &follow_entry.to_str());
        let _r = bin_user.list_remove(&k_v_prev).await?;
        let _r = bin_user.list_remove(&k_v).await?;

        return Ok(());
    }

    /// Returns true when who following whom.
    /// Returns error when who == whom.
    /// Returns error when who or whom has not signed up.
    async fn is_following(&self, who: &str, whom: &str) -> TribResult<bool> {
        // check if who == whom
        if who.to_string() == whom.to_string() {
            let e = TribblerError::Unknown("follow: follow one's self:".to_string() + who);
            return Err(Box::new(e));
        }
        // check if not existing user
        let bin_signup_log = self.bin_storage.bin(SIGNUP_LOG).await?;
        let list = bin_signup_log.list_get(SIGNUP_LOG_LIST).await?;
        let (mut flag_exist1, mut flag_exist2) = (false, false);
        for entry in list.0 {
            let entry_u_and_t = StringAndTime::from_str(&entry);
            if entry_u_and_t.string == who.to_string() {
                flag_exist1 = true;
            }
            if entry_u_and_t.string == whom.to_string() {
                flag_exist2 = true;
            }
        }
        if flag_exist1 == false {
            let e = TribblerError::InvalidUsername(who.to_string());
            return Err(Box::new(e));
        }
        if flag_exist2 == false {
            let e = TribblerError::InvalidUsername(whom.to_string());
            return Err(Box::new(e));
        }

        // check if following
        let bin_user = self.bin_storage.bin(who).await?;
        let list = bin_user.list_get(&follow_list(who.to_string())).await?;
        let mut flag_found = false;
        for entry in list.0 {
            let entry_f = FollowEntry::from_str(&entry);
            if entry_f.whom == whom && entry_f.is_follow {
                flag_found = true;
            }
        }
        return Ok(flag_found);
    }

    /// Returns the list of following users.
    /// Returns error when who has not signed up.
    /// The list have users more than trib.MaxFollowing=2000,
    /// if and only if the 2000'th user is generate d by concurrent Follow()
    /// calls.
    async fn following(&self, who: &str) -> TribResult<Vec<String>> {
        // check if not existing user
        let bin_signup_log = self.bin_storage.bin(SIGNUP_LOG).await?;
        let list = bin_signup_log.list_get(SIGNUP_LOG_LIST).await?;
        let mut flag_exist = false;
        for entry in list.0 {
            let entry_u_and_t = StringAndTime::from_str(&entry);
            if entry_u_and_t.string == who.to_string() {
                flag_exist = true;
            }
        }
        if flag_exist == false {
            let e = TribblerError::InvalidUsername(who.to_string());
            return Err(Box::new(e));
        }

        // return the following list
        let bin_user = self.bin_storage.bin(who).await?;
        let list = bin_user.list_get(&follow_list(who.to_string())).await?;
        let mut list_whom: Vec<String> = Vec::new();
        for entry in list.0 {
            let entry_f = FollowEntry::from_str(&entry);
            if entry_f.is_follow {
                list_whom.push(entry_f.whom);
            }
        }

        return Ok(list_whom);
    }

    /// List the tribs of someone's following users (including himself).
    /// Returns error when user has not signed up.
    async fn home(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        // check if not existing user
        let bin_signup_log = self.bin_storage.bin(SIGNUP_LOG).await?;
        let list = bin_signup_log.list_get(SIGNUP_LOG_LIST).await?;
        let mut flag_exist = false;
        for entry in list.0 {
            let entry_u_and_t = StringAndTime::from_str(&entry);
            if entry_u_and_t.string == user.to_string() {
                flag_exist = true;
            }
        }
        if flag_exist == false {
            let e = TribblerError::InvalidUsername(user.to_string());
            return Err(Box::new(e));
        }

        // get all tribs from this user and its following users
        let mut following = self.following(user).await?;
        following.push(user.to_string());
        let mut all_trib: Vec<TribInfo> = Vec::new();
        for one in following {
            let tribs = self.tribs(&one).await?;
            for trib in tribs {
                let a = (*trib).user.clone();
                let trib_info = TribInfo {
                    user: (*trib).user.clone(),
                    post: (*trib).message.clone(),
                    logical_time: (*trib).clock.clone(),
                    physical_time: (*trib).time.clone(),
                };
                all_trib.push(trib_info);
            }
        }
        all_trib.sort();

        // return the most recent MAX_TRIB_FETCH posts
        let mut trib_list_ret: Vec<Arc<Trib>> = Vec::new();
        for entry in all_trib {
            if trib_list_ret.len() == MAX_TRIB_FETCH {
                break;
            }
            let t = Trib {
                user: entry.user,
                message: entry.post,
                clock: entry.logical_time,
                time: entry.physical_time,
            };
            let n = Arc::new(t);
            trib_list_ret.push(n);
        }
        return Ok(trib_list_ret);
    }
}
