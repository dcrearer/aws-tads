//! Thin wrapper over bundled static `ffmpeg`/`ffprobe` binaries (shelled out, no C linking).
//!
//! The analyzer writes a fetched segment's bytes to a temp file, then uses this crate to probe it
//! and extract the artifacts each analysis needs — audio (for transcription) and sampled frames
//! (for the key-moment vision model). The segment is decoded once; callers fan the artifacts out.
//!
//! Command-argument construction and `ffprobe` JSON parsing are pure and unit-tested; the actual
//! process exec is covered by opt-in integration tests (a real binary must be present).

use std::path::{Path, PathBuf};

use serde::Deserialize;
use tokio::process::Command;

#[derive(Debug, thiserror::Error)]
pub enum VuMediaError {
    #[error("failed to spawn {tool}: {source}")]
    Spawn {
        tool: String,
        source: std::io::Error,
    },
    #[error("{tool} exited with {code}: {stderr}")]
    NonZero {
        tool: String,
        code: String,
        stderr: String,
    },
    #[error("failed to parse ffprobe output: {0}")]
    Probe(String),
}

/// What a segment's media contains, from `ffprobe`.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaInfo {
    pub duration_secs: f64,
    pub has_video: bool,
    pub has_audio: bool,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// Paths to the `ffmpeg`/`ffprobe` binaries. In Lambda these point at the bundled static builds;
/// in dev they default to whatever is on `PATH`.
#[derive(Debug, Clone)]
pub struct Ffmpeg {
    ffmpeg: String,
    ffprobe: String,
}

impl Default for Ffmpeg {
    fn default() -> Self {
        Self {
            ffmpeg: "ffmpeg".into(),
            ffprobe: "ffprobe".into(),
        }
    }
}

impl Ffmpeg {
    pub fn new(ffmpeg: impl Into<String>, ffprobe: impl Into<String>) -> Self {
        Self {
            ffmpeg: ffmpeg.into(),
            ffprobe: ffprobe.into(),
        }
    }

    /// Read `VU_FFMPEG` / `VU_FFPROBE` (set by the Lambda to the bundled binaries), else default.
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            ffmpeg: std::env::var("VU_FFMPEG").unwrap_or(d.ffmpeg),
            ffprobe: std::env::var("VU_FFPROBE").unwrap_or(d.ffprobe),
        }
    }

    /// Probe a media file for duration and stream presence.
    #[tracing::instrument(skip(self))]
    pub async fn probe(&self, input: &Path) -> Result<MediaInfo, VuMediaError> {
        let out = run(&self.ffprobe, &probe_args(input)).await?;
        parse_ffprobe(&out)
    }

    /// Extract a mono WAV at `sample_rate` Hz (16 kHz for ASR). Overwrites `out`.
    #[tracing::instrument(skip(self))]
    pub async fn extract_audio_wav(
        &self,
        input: &Path,
        out: &Path,
        sample_rate: u32,
    ) -> Result<(), VuMediaError> {
        run(&self.ffmpeg, &audio_args(input, out, sample_rate)).await?;
        Ok(())
    }

    /// Sample `fps` frames per second as JPEGs into `out_dir`, returning the written paths in order.
    #[tracing::instrument(skip(self))]
    pub async fn extract_frames(
        &self,
        input: &Path,
        fps: f32,
        out_dir: &Path,
    ) -> Result<Vec<PathBuf>, VuMediaError> {
        tokio::fs::create_dir_all(out_dir)
            .await
            .map_err(|e| VuMediaError::Spawn {
                tool: "mkdir".into(),
                source: e,
            })?;
        let pattern = out_dir.join("frame_%05d.jpg");
        run(&self.ffmpeg, &frame_args(input, fps, &pattern)).await?;
        let mut frames = Vec::new();
        let mut rd = tokio::fs::read_dir(out_dir)
            .await
            .map_err(|e| VuMediaError::Spawn {
                tool: "readdir".into(),
                source: e,
            })?;
        while let Ok(Some(entry)) = rd.next_entry().await {
            let p = entry.path();
            if p.extension().is_some_and(|e| e == "jpg") {
                frames.push(p);
            }
        }
        frames.sort();
        Ok(frames)
    }
}

// ---- pure helpers (unit-tested) ---------------------------------------------------------------

fn probe_args(input: &Path) -> Vec<String> {
    vec![
        "-v".into(),
        "quiet".into(),
        "-print_format".into(),
        "json".into(),
        "-show_format".into(),
        "-show_streams".into(),
        input.to_string_lossy().into_owned(),
    ]
}

fn audio_args(input: &Path, out: &Path, sample_rate: u32) -> Vec<String> {
    vec![
        "-y".into(),
        "-i".into(),
        input.to_string_lossy().into_owned(),
        "-vn".into(),
        "-ac".into(),
        "1".into(),
        "-ar".into(),
        sample_rate.to_string(),
        "-f".into(),
        "wav".into(),
        out.to_string_lossy().into_owned(),
    ]
}

fn frame_args(input: &Path, fps: f32, pattern: &Path) -> Vec<String> {
    vec![
        "-y".into(),
        "-i".into(),
        input.to_string_lossy().into_owned(),
        "-vf".into(),
        format!("fps={fps}"),
        pattern.to_string_lossy().into_owned(),
    ]
}

#[derive(Deserialize)]
struct ProbeOutput {
    #[serde(default)]
    streams: Vec<ProbeStream>,
    format: Option<ProbeFormat>,
}

#[derive(Deserialize)]
struct ProbeStream {
    codec_type: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
}

#[derive(Deserialize)]
struct ProbeFormat {
    duration: Option<String>,
}

fn parse_ffprobe(json: &str) -> Result<MediaInfo, VuMediaError> {
    let out: ProbeOutput =
        serde_json::from_str(json).map_err(|e| VuMediaError::Probe(e.to_string()))?;
    let duration_secs = out
        .format
        .and_then(|f| f.duration)
        .and_then(|d| d.parse::<f64>().ok())
        .unwrap_or(0.0);
    let video = out
        .streams
        .iter()
        .find(|s| s.codec_type.as_deref() == Some("video"));
    let has_audio = out
        .streams
        .iter()
        .any(|s| s.codec_type.as_deref() == Some("audio"));
    Ok(MediaInfo {
        duration_secs,
        has_video: video.is_some(),
        has_audio,
        width: video.and_then(|v| v.width),
        height: video.and_then(|v| v.height),
    })
}

async fn run(tool: &str, args: &[String]) -> Result<String, VuMediaError> {
    let output = Command::new(tool)
        .args(args)
        .output()
        .await
        .map_err(|e| VuMediaError::Spawn {
            tool: tool.to_string(),
            source: e,
        })?;
    if !output.status.success() {
        return Err(VuMediaError::NonZero {
            tool: tool.to_string(),
            code: output
                .status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".into()),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_probe_with_video_and_audio() {
        let json = r#"{
            "streams": [
                { "codec_type": "video", "width": 1920, "height": 1080 },
                { "codec_type": "audio" }
            ],
            "format": { "duration": "4.000000" }
        }"#;
        let info = parse_ffprobe(json).unwrap();
        assert_eq!(info.duration_secs, 4.0);
        assert!(info.has_video && info.has_audio);
        assert_eq!(info.width, Some(1920));
        assert_eq!(info.height, Some(1080));
    }

    #[test]
    fn parses_audio_only() {
        let json = r#"{ "streams": [{ "codec_type": "audio" }], "format": { "duration": "2.5" } }"#;
        let info = parse_ffprobe(json).unwrap();
        assert!(!info.has_video && info.has_audio);
        assert_eq!(info.duration_secs, 2.5);
        assert_eq!(info.width, None);
    }

    #[test]
    fn missing_duration_defaults_zero() {
        let info = parse_ffprobe(r#"{ "streams": [] }"#).unwrap();
        assert_eq!(info.duration_secs, 0.0);
        assert!(!info.has_video && !info.has_audio);
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_ffprobe("not json").is_err());
    }

    #[test]
    fn arg_builders_are_well_formed() {
        let a = audio_args(Path::new("/tmp/in.ts"), Path::new("/tmp/out.wav"), 16000);
        assert!(a.windows(2).any(|w| w == ["-ar", "16000"]));
        assert!(a.contains(&"-vn".to_string()) && a.last().unwrap() == "/tmp/out.wav");

        let f = frame_args(Path::new("/tmp/in.ts"), 1.0, Path::new("/tmp/f_%05d.jpg"));
        assert!(f.windows(2).any(|w| w[0] == "-vf" && w[1] == "fps=1"));

        let p = probe_args(Path::new("/tmp/in.ts"));
        assert!(p.contains(&"json".to_string()) && p.last().unwrap() == "/tmp/in.ts");
    }
}
