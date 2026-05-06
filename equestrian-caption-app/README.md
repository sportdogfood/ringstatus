# Equestrian Caption App

Mobile-first caption builder for Lainey-style horse posts. The app helps choose a post type, add image notes, generate four caption options, save one, copy saved captions, and track monthly post counts.

## What It Does

- `Create`: choose a post type, upload/select an image, describe the moment, generate 4 captions, and save one.
- `Log`: review saved captions with photo thumbnails and copy captions.
- `Dashboard`: compare saved posts against the expected monthly mix.

Caption generation is local and rule-based. It does not call an AI API yet.

## Run Locally

```powershell
cd "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\equestrian-caption-app"
npm install
npm run dev
```

Open:

```text
http://localhost:5173/
```

## Verify

```powershell
npm run build
npm run lint
```

## Voice Engine

The voice system lives in [src/App.tsx](./src/App.tsx):

- `voiceProfile`: global voice, banned phrases, recurring series, highlighted Lainey lines, and output rules.
- `postTypeRules`: per-topic purpose, structure, use cases, avoid rules, and line limits.
- `starterPools`: caption seed lines used for each post type.
- `highlightedLinesByPostType`: required Lainey-highlighted lines available by topic.
- `generateCaptions`: rotates starter pools and guarantees each set of four includes one highlighted line.

## Post Types

- `what i see`: subtle rider-eye detail, like base, canter, rhythm, softness, waiting, or rideability.
- `what the horse sees`: dry horse POV without baby talk or meme tone.
- `what we did`: training/progress caption showing useful work without coach voice.
- `what we almost did`: imperfect result with perspective, no excuses or bitterness.
- `reality`: working-student barn truth, one real sentence, no martyr energy.
- `confidence`: very short, earned, direct captions.

## Style

The UI follows the tap-active cadence from `../docs/tapactive-kit.css`:

- fixed phone canvas
- dark radial app shell
- compact header
- pill rows and chips
- bottom nav
- fast tap feedback
- sticky action row for saving selected captions

The local UI primitives are in [src/components/ui](./src/components/ui).

## Current Limits

- Saved posts live only in React state, so they reset on reload.
- Caption generation is template/rule-based, not model-backed.
- Uploaded images are kept as data URLs in local component state only.
