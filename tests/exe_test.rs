use std::process::{Command, Stdio};

// Path to executable set by cargo test:
// See: https://doc.rust-lang.org/cargo/commands/cargo-test.html#target-selection
const KVBENCH_EXE_PATH: &str = env!("CARGO_BIN_EXE_kvbench");

// Test the process starts to verify command line parsing.
#[test]
fn test_exe_runs() -> Result<(), Box<dyn std::error::Error>> {
    let subprocess = Command::new(KVBENCH_EXE_PATH)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("--num-keys=1")
        .arg("--measure-duration=1s")
        .spawn()?;

    let output = subprocess.wait_with_output()?;
    assert!(output.status.success());

    let stdout_string = String::from_utf8(output.stdout)?;
    let stderr_string = String::from_utf8(output.stderr)?;
    assert!(stdout_string.contains(" requests/sec"));
    assert_eq!("", stderr_string);

    Ok(())
}
