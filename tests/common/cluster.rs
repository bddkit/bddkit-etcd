//! A three-node etcd cluster of real processes on free ports, which a test can
//! break: kill a node, start it again. It is processes and not compose so that
//! it runs where Docker does not, and so that a test breaks the binary the way
//! a server team would. The binary comes from `BDDKIT_TEST_ETCD_BIN` or `PATH`;
//! without one the test skips, except under `CI`, where a skipped suite must
//! never look green.
//!
//! Dropping the cluster kills and reaps every node, so a test that fails
//! leaves no etcd behind. A test process that is itself killed cannot clean up.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// The etcd binary of the tests, if there is one.
pub fn etcd_bin() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("BDDKIT_TEST_ETCD_BIN") {
        let path = PathBuf::from(path);
        assert!(path.is_file(), "BDDKIT_TEST_ETCD_BIN is not a file");
        return Some(path);
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join("etcd"))
        .find(|candidate| candidate.is_file())
}

/// Returns from the test, with a note, when there is no etcd binary to run it on.
#[macro_export]
macro_rules! require_etcd_bin {
    () => {
        match $crate::common::cluster::etcd_bin() {
            Some(bin) => bin,
            None if std::env::var_os("CI").is_some() => {
                panic!("no etcd binary (BDDKIT_TEST_ETCD_BIN or PATH), and CI must not skip")
            }
            None => {
                eprintln!("SKIP: set BDDKIT_TEST_ETCD_BIN to an etcd binary");
                return;
            }
        }
    };
}

struct Member {
    name: String,
    client: u16,
    peer: u16,
    child: Option<Child>,
}

pub struct Cluster {
    bin: PathBuf,
    dir: PathBuf,
    members: Vec<Member>,
}

/// `n` ports that were free a moment ago.
fn free_ports(n: usize) -> Vec<u16> {
    let held: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("a free port"))
        .collect();
    held.iter()
        .map(|l| l.local_addr().expect("address").port())
        .collect()
}

impl Cluster {
    pub fn start(bin: &Path) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        // A port is free only until the next test asks for one, so the tests
        // of one binary start their clusters one at a time.
        static STARTING: Mutex<()> = Mutex::new(());
        let _starting = STARTING.lock().unwrap_or_else(PoisonError::into_inner);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("bddkit-etcd-cluster-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("cluster dir");
        let ports = free_ports(6);
        let members = (0..3)
            .map(|i| Member {
                name: format!("n{}", i + 1),
                client: ports[i],
                peer: ports[3 + i],
                child: None,
            })
            .collect();
        let mut cluster = Self {
            bin: bin.to_path_buf(),
            dir,
            members,
        };
        for node in 0..3 {
            cluster.spawn(node);
        }
        cluster.wait_for_leader();
        cluster
    }

    pub fn endpoint(&self, node: usize) -> String {
        format!("http://127.0.0.1:{}", self.members[node].client)
    }

    pub fn pids(&self) -> Vec<u32> {
        self.members
            .iter()
            .filter_map(|m| m.child.as_ref().map(Child::id))
            .collect()
    }

    fn spawn(&mut self, node: usize) {
        let member = &self.members[node];
        let url = |port: u16| format!("http://127.0.0.1:{port}");
        let initial = self
            .members
            .iter()
            .map(|m| format!("{}={}", m.name, url(m.peer)))
            .collect::<Vec<_>>()
            .join(",");
        let log =
            std::fs::File::create(self.dir.join(format!("{}.log", member.name))).expect("log file");
        // Short raft timings: a node that lost its quorum notices in about a
        // second, so a test does not wait for the defaults.
        let child = Command::new(&self.bin)
            .args(["--name", &member.name])
            .arg("--data-dir")
            .arg(self.dir.join(&member.name))
            .args(["--listen-client-urls", &url(member.client)])
            .args(["--advertise-client-urls", &url(member.client)])
            .args(["--listen-peer-urls", &url(member.peer)])
            .args(["--initial-advertise-peer-urls", &url(member.peer)])
            .args(["--initial-cluster", &initial])
            .args(["--initial-cluster-token", "bddkit-etcd-test"])
            .args(["--heartbeat-interval", "100", "--election-timeout", "1000"])
            .stdin(Stdio::null())
            .stdout(log.try_clone().expect("log"))
            .stderr(log)
            .spawn()
            .expect("start etcd");
        self.members[node].child = Some(child);
    }

    /// Kills a node, and returns once it is gone.
    pub fn kill(&mut self, node: usize) {
        if let Some(mut child) = self.members[node].child.take() {
            child.kill().expect("kill etcd");
            child.wait().expect("reap etcd");
        }
    }

    /// The same node on the same data directory: it rejoins with what it had.
    pub fn restart(&mut self, node: usize) {
        self.kill(node);
        self.spawn(node);
    }

    /// Every running node answers `Status` and knows a leader.
    fn wait_for_leader(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        for node in 0..self.members.len() {
            while !self.leader_known(node) {
                self.assert_running();
                assert!(
                    Instant::now() < deadline,
                    "node {node} has no leader after 30 s, see the logs in {}",
                    self.dir.display()
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    /// A node that exited (a port taken, say) is named at once, with the end
    /// of its log, instead of after the whole wait.
    fn assert_running(&mut self) {
        for member in &mut self.members {
            let Some(status) = member
                .child
                .as_mut()
                .and_then(|c| c.try_wait().ok().flatten())
            else {
                continue;
            };
            let log = std::fs::read_to_string(self.dir.join(format!("{}.log", member.name)))
                .unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(15).collect();
            panic!(
                "etcd {} exited during startup ({status}):\n{}",
                member.name,
                tail.into_iter().rev().collect::<Vec<_>>().join("\n")
            );
        }
    }

    fn leader_known(&self, node: usize) -> bool {
        let endpoint = self.endpoint(node);
        super::block_on(async {
            let Ok(client) = etcd_client::Client::connect([endpoint], None).await else {
                return false;
            };
            client
                .maintenance_client()
                .status()
                .await
                .is_ok_and(|status| status.leader() != 0)
        })
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in 0..self.members.len() {
            self.kill(node);
        }
        // The logs of a failing test are what explains it.
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}
