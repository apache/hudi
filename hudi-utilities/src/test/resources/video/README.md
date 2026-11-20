<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Video and audio test clips

Every clip here is synthetic or public domain. A clip not listed below is made by the `generate-*.sh` script next to
it. The base clip below is committed as is (no script); the derived clips are made by `generate-av-fixtures.sh` from it
and from a public-domain LibriVox recording.

## Sources

- `h264-aac-bframes-12s.mp4`, the base clip: 12 s, 160x120, 10 fps, H.264 High (libx264, CRF 23, a keyframe every 2 s,
  3 B-frames), with a 16 kHz mono AAC track of a steady 440 Hz sine. The picture is a synthetic pattern: random colored
  4x4 cells with a horizontal line from 0 s to 4 s and from 8 s to 12 s, and a still grey frame with a white circle from
  4 s to 8 s, so the keyframes at 4 s and 6 s are identical. It carries no third-party material.
- Speech: 60.0 s to 72.0 s of the LibriVox recording of "The Selfish Giant" by Oscar Wilde, read by Joy Chan,
  <https://archive.org/download/hpot_librivox/hpot_03_selfish_giant.mp3> (SHA-256
  b12913bc2655b02bc8649766c506cc862e790d613afbc5dd6cceec0f5c70540f). LibriVox recordings are in the public domain.

## Derived clips (`generate-av-fixtures.sh`)

| Clip | Made from | How |
|---|---|---|
| `h264-aac-bframes-trimmed-ss3.mp4` | base clip | cut at 3 s (both tracks), stream copy |
| `h264-aac-bframes-video-delayed-1500ms.mp4` | base clip | video track delayed 1.5 s by an empty edit, stream copy |
| `h264-aac-speech-12s.mp4` | base clip video + speech | the speech excerpt (44.1 kHz stereo) encoded as AAC 64 kb/s, video stream copy |

With the linux-arm64 ffmpeg program of org.bytedeco:ffmpeg:7.1-1.5.11 the script rewrites the committed bytes of every
derived clip; the AAC encoding of the speech track may differ on another platform.
