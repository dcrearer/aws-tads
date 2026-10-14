//! Media assembly: turn the window's segment media into one MP4 clip Pegasus can read.
//!
//! TAMS segments are container fragments (e.g. MPEG-TS) that aren't necessarily decodable as-is by
//! the model, so we **always** run them through ffmpeg to produce a single standard MP4 — even for
//! a one-segment window (remux of one input). Abstracted behind [`MediaAssembler`] so the worker is
//! testable with a trivial concatenator; the production [`FfmpegAssembler`] bundles ffmpeg in the
//! Lambda artifact.

use std::future::Future;
use std::io::Write;
use std::process::Command;

/// Combine ordered segment media blobs into one playable MP4 clip.
pub trait MediaAssembler: Send + Sync {
    fn assemble(&self, clips: &[Vec<u8>]) -> impl Future<Output = Result<Vec<u8>, String>> + Send;
}

/// Production assembler: ffmpeg concat demuxer → MP4 (video stream-copied, audio normalised to AAC).
#[derive(Debug, Clone)]
pub struct FfmpegAssembler {
    ffmpeg: String,
}

impl Default for FfmpegAssembler {
    fn default() -> Self {
        Self {
            ffmpeg: std::env::var("FFMPEG_PATH").unwrap_or_else(|_| "ffmpeg".to_string()),
        }
    }
}

impl MediaAssembler for FfmpegAssembler {
    async fn assemble(&self, clips: &[Vec<u8>]) -> Result<Vec<u8>, String> {
        if clips.is_empty() {
            return Err("no media to assemble".to_string());
        }
        remux_to_mp4(&self.ffmpeg, clips)
    }
}

/// Write each clip to a temp file, build a concat list, and remux into one MP4. Works for a single
/// input (plain remux) or many (concatenation). Video is stream-copied; audio is re-encoded to AAC
/// so containers whose audio codec MP4 rejects (e.g. MP2 in MPEG-TS) still succeed.
fn remux_to_mp4(ffmpeg: &str, clips: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let dir = std::env::temp_dir().join(format!("vu-clip-{}", std::process::id()));
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let _guard = DirGuard(dir.clone());

    let mut list = String::new();
    for (i, clip) in clips.iter().enumerate() {
        // Extensionless: let ffmpeg probe the real container rather than trust a name.
        let path = dir.join(format!("seg-{i}"));
        std::fs::File::create(&path)
            .and_then(|mut f| f.write_all(clip))
            .map_err(|e| e.to_string())?;
        list.push_str(&format!("file '{}'\n", path.display()));
    }
    let list_path = dir.join("list.txt");
    std::fs::write(&list_path, list).map_err(|e| e.to_string())?;
    let out_path = dir.join("out.mp4");

    let output = Command::new(ffmpeg)
        .args([
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            &list_path.display().to_string(),
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-movflags",
            "+faststart",
            &out_path.display().to_string(),
        ])
        .output()
        .map_err(|e| format!("failed to run ffmpeg (is it installed / FFMPEG_PATH set?): {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "ffmpeg remux failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    std::fs::read(&out_path).map_err(|e| e.to_string())
}

/// Best-effort temp-dir cleanup.
struct DirGuard(std::path::PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
