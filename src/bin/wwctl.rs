use std::{
    env, fs,
    io::{self, Write},
};

use anyhow::{bail, Context, Result};
use finnstream::{
    admin::{AdminClient, CLIENT_API_KEY_ENV},
    control::ControlExecution,
};

struct CliOptions {
    endpoint: String,
    api_key: Option<String>,
    script: Option<String>,
    json: bool,
}

impl CliOptions {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Option<Self>> {
        let mut endpoint =
            env::var("WHITEWATER_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:7071".to_owned());
        let mut api_key = env::var(CLIENT_API_KEY_ENV).ok();
        let mut script = None;
        let mut json = false;
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--endpoint" => {
                    endpoint = arguments.next().context("--endpoint requires a URL")?;
                }
                "--api-key" => {
                    api_key = Some(arguments.next().context("--api-key requires a value")?);
                }
                "-e" | "--execute" => {
                    script = Some(
                        arguments
                            .next()
                            .context("--execute requires a WCL statement")?,
                    );
                }
                "-f" | "--file" => {
                    let path = arguments.next().context("--file requires a path")?;
                    script = Some(
                        fs::read_to_string(&path)
                            .with_context(|| format!("failed to read {path}"))?,
                    );
                }
                "--json" => json = true,
                "-h" | "--help" => return Ok(None),
                value => bail!("unknown argument {value}; use --help"),
            }
        }
        Ok(Some(Self {
            endpoint,
            api_key,
            script,
            json,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let Some(options) = CliOptions::parse(env::args().skip(1))? else {
        print_help();
        return Ok(());
    };
    let api_key = options.api_key.with_context(|| {
        format!("provide --api-key or set the {CLIENT_API_KEY_ENV} environment variable")
    })?;
    let client = AdminClient::new(options.endpoint, api_key);
    if let Some(script) = options.script {
        let result = client.execute_wcl(script).await?;
        print_execution(&result, options.json)?;
    } else {
        run_shell(&client, options.json).await?;
    }
    Ok(())
}

async fn run_shell(client: &AdminClient, mut json: bool) -> Result<()> {
    println!("Whitewater Control Language shell");
    println!("All commands use the authenticated Admin API. Type \\help for help.");
    let mut statement = String::new();
    loop {
        print!(
            "{}",
            if statement.is_empty() {
                "whitewater> "
            } else {
                "       ...> "
            }
        );
        io::stdout().flush()?;
        let mut line = String::new();
        if io::stdin().read_line(&mut line)? == 0 {
            println!();
            return Ok(());
        }
        let trimmed = line.trim();
        if statement.is_empty() && trimmed.starts_with('\\') {
            match trimmed {
                "\\q" | "\\quit" | "\\exit" => return Ok(()),
                "\\help" | "\\h" => print_shell_help(),
                "\\json" => {
                    json = !json;
                    println!("JSON output {}", if json { "enabled" } else { "disabled" });
                }
                "\\clear" => statement.clear(),
                "\\g" => println!("No buffered statement."),
                command => println!("Unknown shell command: {command}"),
            }
            continue;
        }
        if trimmed == "\\g" {
            if statement.trim().is_empty() {
                println!("No buffered statement.");
                continue;
            }
        } else {
            statement.push_str(&line);
        }
        if trimmed != "\\g" && !statement_complete(&statement) {
            continue;
        }
        match client.execute_wcl(statement.trim()).await {
            Ok(result) => print_execution(&result, json)?,
            Err(error) => eprintln!("Error: {error}"),
        }
        statement.clear();
    }
}

fn statement_complete(statement: &str) -> bool {
    let mut quoted = false;
    let mut last_unquoted = None;
    let mut chars = statement.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '\'' if quoted && chars.peek() == Some(&'\'') => {
                chars.next();
            }
            '\'' => quoted = !quoted,
            character if !quoted && !character.is_whitespace() => last_unquoted = Some(character),
            _ => {}
        }
    }
    !quoted && last_unquoted == Some(';')
}

fn print_execution(execution: &ControlExecution, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(execution)?);
        return Ok(());
    }
    for result in &execution.results {
        println!("{}: {}", result.statement, result.message);
        if !result.data.is_null() {
            println!("{}", serde_json::to_string_pretty(&result.data)?);
        }
    }
    println!("revision: {} ({})", execution.revision, execution.authority);
    if !execution.warning.is_empty() {
        println!("warning: {}", execution.warning);
    }
    Ok(())
}

fn print_help() {
    println!(
        "wwctl - authenticated Whitewater Admin API shell\n\n\
         Usage:\n  wwctl [--endpoint URL] [--api-key KEY]\n  wwctl [--endpoint URL] [--api-key KEY] --execute \"WCL;\"\n  wwctl [--endpoint URL] [--api-key KEY] --file commands.wcl\n\n\
         Options:\n  --json                Print complete JSON responses\n  -e, --execute WCL     Execute one WCL script and exit\n  -f, --file PATH       Execute a WCL file and exit\n\n\
         Environment:\n  WHITEWATER_ENDPOINT  Defaults to http://127.0.0.1:7071\n  WHITEWATER_API_KEY   Admin API Bearer key"
    );
}

fn print_shell_help() {
    println!(
        "Enter WCL ending with a semicolon. Multi-line statements are supported.\n\
         \\g      Execute the buffered statement without a trailing semicolon\n\
         \\clear  Clear the buffered statement\n\
         \\json   Toggle full JSON output\n\
         \\help   Show this help\n\
         \\quit   Exit the shell"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_complete_statements_without_terminating_inside_quotes() {
        assert!(statement_complete("SHOW FEEDS;"));
        assert!(!statement_complete("SHOW FEEDS"));
        assert!(!statement_complete("SEEK READER audit TO CURSOR 'abc;def'"));
        assert!(statement_complete("SEEK READER audit TO CURSOR 'abc;def';"));
    }

    #[test]
    fn parses_one_shot_and_interactive_modes() {
        let interactive = CliOptions::parse(Vec::<String>::new()).unwrap().unwrap();
        assert!(interactive.script.is_none());

        let one_shot = CliOptions::parse(vec![
            "--api-key".to_owned(),
            "development-key-long-enough".to_owned(),
            "--json".to_owned(),
            "--execute".to_owned(),
            "SHOW FEEDS;".to_owned(),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(one_shot.script.as_deref(), Some("SHOW FEEDS;"));
        assert!(one_shot.json);
    }
}
