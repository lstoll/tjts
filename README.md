# tjts

A timeshifting radio proxy. It records ABC's live HLS streams (Triple J, Double J) and replays them shifted to the listener's timezone, so breakfast radio plays at breakfast wherever you are.

## Endpoints

All take `stream=<id>&tz=<IANA timezone>`, e.g. `?stream=triplej&tz=Europe/Berlin`.

| Path | What |
|------|------|
| `/` | Web player with now-playing and track history |
| `/icecast` | Plain AAC stream with ICY metadata. Use this for Sonos, VLC, etc. |
| `/m3u8` | HLS playlist |
| `/nowplaying` | JSON: current track and recent history on the listener's timeline |

## Now playing

Streams with a `nowPlaying` block get track info from ABC's public play log (`music.abcradio.net.au/api/v1/plays`):

```yaml
streams:
  - id: triplej
    # ...
    nowPlaying:
      station: triplej   # ABC service id
      # lag: 5s          # optional trim; positive makes titles change later
```

- A poller fetches plays every minute and stores them per stream in S3 at `_meta/plays/<stream>.json`. On startup it loads that file, then backfills any gap from the API (which supports time ranges), so no separate backfill step is needed.
- Timing needs no tuning. ABC's playlists carry `EXT-X-PROGRAM-DATE-TIME`, so the fetcher measures how long each new segment took to reach us (median of recent segments, exported as `tjts_segment_latency_seconds`). A chunk's fetch time minus that latency is when its audio went to air, and the current track is the play on air at that moment. Titles follow each listener's offset.
- `/icecast` sends `StreamTitle='Artist - Title'` every 16000 bytes when the client sends `Icy-MetaData: 1`. Between tracks it sends the station name. Players show a title when it arrives, and audio is sent up to 30s ahead, so each title is for the audio the listener is hearing as the block is written, not the audio packed around it.
- `lag` is an optional trim on top of the measurement. Positive values make titles change later.

## Running locally

`docker compose up` starts MinIO, then:

```
go run . -config config.yaml
```
