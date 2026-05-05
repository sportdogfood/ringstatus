"use client";

import React, { useMemo, useState } from "react";
import {
  Camera,
  Check,
  Copy,
  Image as ImageIcon,
  LayoutDashboard,
  List,
  RefreshCcw,
  Sparkles,
} from "lucide-react";
import { motion } from "framer-motion";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";

const postTypes = [
  { value: "what i see", expected: 8 },
  { value: "what the horse sees", expected: 6 },
  { value: "what we did", expected: 8 },
  { value: "what we almost did", expected: 5 },
  { value: "reality", expected: 6 },
  { value: "confidence", expected: 4 },
] as const;

type PostType = (typeof postTypes)[number]["value"];
type Screen = "create" | "log" | "dashboard";

type GeneratedCaption = {
  id: string;
  text: string;
};

type SavedPost = {
  id: string;
  postType: PostType;
  description: string;
  caption: string;
  photoName: string;
  photoUrl: string;
  createdAt: string;
  monthKey: string;
};

type DashboardRow = {
  monthKey: string;
  counts: Record<PostType, number>;
  total: number;
  expected: number;
};

type VoiceProfile = {
  identity: string[];
  coreTone: string[];
  feel: string[];
  formatting: string[];
  preferredQualities: string[];
  avoid: string[];
  bannedPhrases: string[];
  phrasingStyle: string[];
  knobs: Record<string, string>;
  generationChecklist: {
    should: string[];
    shouldNot: string[];
  };
  manualOverrides: Record<string, string[]>;
};

type PostTypeRule = {
  purpose: string[];
  bestUsedFor: string[];
  leadWith: string[];
  structure: string[];
  emotionalRange: string[];
  allowedQualities: string[];
  avoid: string[];
  endingStyle: string[];
  variationKnobs: Record<string, string[]>;
  manualPrompts: string[];
  maxLines: number;
};

const voiceProfile: VoiceProfile = {
  identity: [
    "15 years old",
    "knows a lot about horses",
    "rides many different horses",
    "working student",
    "mentally tough",
    "observant",
    "understated",
    "not attention-seeking",
    "not ribbon-centered",
    "values feel, improvement, and connection",
  ],
  coreTone: [
    "casual",
    "horse-smart",
    "a little dry",
    "slightly funny",
    "self-aware",
    "honest",
    "quietly confident",
    "not dramatic",
    "not polished",
    "not fake-deep",
  ],
  feel: [
    "like texting a horse friend your age",
    "like a real rider, not a brand",
    "like someone who notices things other people miss",
    "like someone who works",
    "like someone who does not need to oversell anything",
  ],
  formatting: [
    "lowercase is okay",
    "short captions are preferred",
    "1 to 3 lines is ideal",
    "sentence fragments are okay",
    "not every caption needs full grammar",
    "short ending lines work well",
  ],
  preferredQualities: [
    "specific",
    "real",
    "simple",
    "horsey",
    "understated",
    "natural",
    "not overexplained",
    "not too polished",
  ],
  avoid: [
    "cheesy horse-girl language",
    "adult-sounding language",
    "influencer language",
    "bragging",
    "fake humility",
    "bitterness",
    "motivational quote energy",
    "stale stock caption language",
    "anything that sounds generated",
  ],
  bannedPhrases: [
    "so proud",
    "grateful beyond words",
    "soulful partner",
    "my heart horse",
    "every ride is a blessing",
    "not the result we wanted but",
    "could not be more proud",
    "this one means so much",
  ],
  phrasingStyle: [
    "simple words",
    "clean observations",
    "subtle humor",
    "dry reactions",
    "real horse details",
    "calm endings",
    "quiet confidence",
  ],
  knobs: {
    confidence: "medium",
    humor: "medium",
    softness: "medium-low",
    maturity: "teen",
    polish: "low",
    detail: "medium-high",
    bragFilter: "high",
    bitternessFilter: "very high",
    length: "short",
  },
  generationChecklist: {
    should: [
      "sound like a teenager who knows horses",
      "sound natural",
      "feel specific to the image or ride",
      "avoid stale stock phrasing",
      "avoid adult polish",
      "match the post type",
      "stay fairly short",
    ],
    shouldNot: [
      "sound like a brand",
      "sound like a trainer report",
      "sound fake-deep",
      "sound bitter",
      "sound like it was written to impress adults",
      "sound copied from generic horse Instagram",
    ],
  },
  manualOverrides: {
    "words i use all the time on purpose": [],
    "words i am sick of": [],
    "phrases that sound most like me": [],
    "phrases that never sound like me": [],
    "horse words i like": [],
    "horse words i do not like": [],
    "captions i wrote that felt most like me": [],
    "captions i wrote that felt too adult / fake / generated": [],
  },
};

const postTypeRules: Record<PostType, PostTypeRule> = {
  "what i see": {
    purpose: [
      "show rider awareness",
      "highlight a subtle detail",
      "sound observant and horsey",
      "make the caption about what mattered, not what was flashy",
    ],
    bestUsedFor: [
      "base",
      "canter",
      "rhythm",
      "changes",
      "softness",
      "feel",
      "rideability",
      "waiting",
      "straightness",
      "subtle improvement",
    ],
    leadWith: [
      "one real detail you noticed",
      "one thing that changed",
      "one thing most non-horse people would miss",
    ],
    structure: ["observation", "why it mattered", "understated ending"],
    emotionalRange: ["calm", "observant", "quietly pleased", "thoughtful", "not emotional"],
    allowedQualities: ["detail-first", "rider-eye language", "subtle pride", "quiet satisfaction"],
    avoid: [
      "big emotions",
      "best ever language",
      "fake-deep meaning",
      "ribbon talk unless truly needed",
      "generic great round wording",
    ],
    endingStyle: ["short", "understated", "almost quiet"],
    variationKnobs: {
      observationSharpness: ["low", "medium", "high"],
      horseSpecificDetail: ["low", "medium", "high"],
      softness: ["low", "medium", "high"],
      humor: ["off", "light", "medium"],
    },
    manualPrompts: [
      "favorite opening words",
      "details i use a lot",
      "details i never want repeated too often",
      "phrases that sound most like me here",
      "phrases to block here",
    ],
    maxLines: 3,
  },
  "what the horse sees": {
    purpose: [
      "let the horse narrate",
      "make it funny, dry, or honest",
      "show personality without being cheesy",
    ],
    bestUsedFor: [
      "silly faces",
      "weird timing",
      "horse opinions",
      "chaotic moments",
      "your horse tolerating you",
      "playful barn situations",
    ],
    leadWith: [
      "the horse's imagined opinion",
      "the horse's reaction to you",
      "a dry observation from the horse's side",
    ],
    structure: ["horse voice line", "short follow-up line if needed"],
    emotionalRange: ["funny", "dry", "mildly sarcastic", "playful", "affectionate if subtle"],
    allowedQualities: ["personality", "judgment", "understatement", "horse humor"],
    avoid: [
      "baby talk",
      "cringe animal voice",
      "overhumanizing",
      "therapy-horse language",
      "trying too hard to be funny",
    ],
    endingStyle: ["quick", "punchy", "deadpan if possible"],
    variationKnobs: {
      humor: ["low", "medium", "high"],
      sarcasm: ["off", "light", "medium"],
      affection: ["low", "medium", "high"],
      chaosLevel: ["low", "medium", "high"],
    },
    manualPrompts: [
      "horse voice should feel like",
      "horse voice should never sound like",
      "favorite kinds of jokes here",
      "joke styles to avoid",
      "best horse-personality words",
    ],
    maxLines: 2,
  },
  "what we did": {
    purpose: ["show work", "show progress", "show what improved", "show that something useful happened"],
    bestUsedFor: [
      "training rides",
      "progress posts",
      "before/after feeling",
      "helping a horse improve",
      "calm, productive days",
      "horses you are bringing along",
    ],
    leadWith: ["what you focused on", "what the horse needed", "what got better"],
    structure: ["one focus", "one shift or improvement", "grounded ending"],
    emotionalRange: ["steady", "productive", "confident", "practical", "not showy"],
    allowedQualities: ["useful", "horse-first", "clear", "grounded", "quietly proud"],
    avoid: [
      "sounding like a lesson note from an adult trainer",
      "overselling progress",
      "bragging about fixing the horse",
      "sounding too polished or clinical",
    ],
    endingStyle: ["calm", "earned", "useful"],
    variationKnobs: {
      progressEmphasis: ["low", "medium", "high"],
      horseImprovementEmphasis: ["low", "medium", "high"],
      riderCreditEmphasis: ["low", "medium", "high"],
      warmth: ["low", "medium", "high"],
    },
    manualPrompts: [
      "progress words i like",
      "progress words i hate",
      "ways i like to describe improvement",
      "words that sound too trainer-ish",
      "favorite endings for this type",
    ],
    maxLines: 3,
  },
  "what we almost did": {
    purpose: [
      "make a non-winning result still meaningful",
      "frame progress without self-pity",
      "show maturity and perspective",
    ],
    bestUsedFor: [
      "no ribbon",
      "one mistake",
      "almost there rounds",
      "not pretty but productive rides",
      "improvement without result",
    ],
    leadWith: ["what did not happen", "what still mattered more"],
    structure: ["missed outcome", "meaningful gain", "calm close"],
    emotionalRange: ["honest", "grounded", "mature", "slightly disappointed if true", "never dramatic"],
    allowedQualities: ["perspective", "progress", "calm honesty", "mental toughness"],
    avoid: [
      "pity",
      "bitterness",
      "excuses",
      "defensive tone",
      "almost only counts energy",
      "talking down about the horse",
    ],
    endingStyle: ["steady", "quiet", "meaningful"],
    variationKnobs: {
      disappointmentLevel: ["low", "medium", "high"],
      optimismLevel: ["low", "medium", "high"],
      progressEmphasis: ["low", "medium", "high"],
      toughnessLevel: ["low", "medium", "high"],
    },
    manualPrompts: [
      "ways i like to say it did not happen",
      "ways i like to say it still mattered",
      "phrases that feel too negative",
      "phrases that feel too fake-positive",
      "go-to closes for this type",
    ],
    maxLines: 3,
  },
  reality: {
    purpose: [
      "show the actual day",
      "show working-student life",
      "show effort and barn truth without making it a speech",
    ],
    bestUsedFor: [
      "long days",
      "multiple horses",
      "feed / chores / riding",
      "tired legs",
      "wet boots",
      "barn dust",
      "chaos",
      "unglamorous moments",
      "silly but true moments",
    ],
    leadWith: ["what the day felt like", "something physical or sensory", "one real detail from the day"],
    structure: ["sensory reality", "simple tag or close"],
    emotionalRange: ["tired", "honest", "funny", "dry", "matter-of-fact"],
    allowedQualities: [
      "barn realism",
      "effort",
      "humor",
      "work ethic shown indirectly",
      "texture and sensory detail",
    ],
    avoid: [
      "whining",
      "martyr tone",
      "trying to prove you work harder than everyone",
      "long speeches about sacrifice",
    ],
    endingStyle: ["short", "real", "maybe funny", "maybe soft"],
    variationKnobs: {
      workEmphasis: ["low", "medium", "high"],
      humor: ["low", "medium", "high"],
      tiredness: ["low", "medium", "high"],
      sensoryDetail: ["low", "medium", "high"],
    },
    manualPrompts: [
      "sensory details i use a lot",
      "reality details i want more of",
      "things that feel too complain-y",
      "favorite barn words / images",
      "best short closes here",
    ],
    maxLines: 3,
  },
  confidence: {
    purpose: ["say very little, strongly", "feel earned, not loud", "let the image carry the weight"],
    bestUsedFor: [
      "strongest images",
      "simple wins",
      "useful rides",
      "quiet pride",
      "strong single-photo posts",
      "when the caption should stay short",
    ],
    leadWith: ["the takeaway", "the feeling", "the one phrase that says enough"],
    structure: ["one line", "maybe two", "no explanation unless needed"],
    emotionalRange: ["quiet", "strong", "final", "earned", "restrained"],
    allowedQualities: ["understatement", "firmness", "calm confidence", "directness"],
    avoid: ["quote energy", "empowerment language", "trying to sound deep", "anything that explains too much"],
    endingStyle: ["stop early", "let it sit", "no extra sentence unless it improves it"],
    variationKnobs: {
      intensity: ["low", "medium", "high"],
      brevity: ["low", "medium", "very high"],
      confidenceLevel: ["low", "medium", "high"],
      softness: ["low", "medium", "high"],
    },
    manualPrompts: [
      "favorite one-liners",
      "favorite two-line patterns",
      "words that feel strongest",
      "words that feel too much",
      "confidence captions i actually like",
    ],
    maxLines: 2,
  },
};

const starterPools: Record<PostType, string[]> = {
  "what i see": [
    "he waited without getting flat. finally.",
    "the base showed up because the canter stayed normal.",
    "she got softer after the turn, which was the whole point.",
    "less noise in the bridle today.",
    "he stayed straight enough to actually hear me.",
    "the change was late, but the canter did not fall apart.",
    "not flashy. just a better feel.",
    "she let me keep the rhythm instead of negotiating every stride.",
  ],
  "what the horse sees": [
    "she said relax. i considered it.",
    "i had thoughts. she kept riding.",
    "apparently steering is still on the schedule.",
    "she wanted organized. i offered decorative.",
    "i did not make it simple. she was annoyingly patient.",
    "the jump was fine. my opinion was larger.",
    "she asked nicely. i answered eventually.",
    "today i provided character development.",
  ],
  "what we did": [
    "kept the canter quieter and got a better answer.",
    "worked on waiting without losing the step.",
    "made it less complicated, which helped.",
    "got him softer after the first few minutes.",
    "put the rhythm first and the rest got easier.",
    "kept it boring on purpose.",
    "asked for straight, got closer.",
    "not fixed. just better.",
  ],
  "what we almost did": [
    "missed the result, kept the progress.",
    "one rail, better ride.",
    "not the round on paper. still a useful one.",
    "almost had it, learned where it left.",
    "not pretty the whole way. better by the end.",
    "the mistake was loud. the answer after was better.",
    "no prize, but not a wasted trip.",
    "close enough to be annoying. useful enough to keep.",
  ],
  reality: [
    "hay in my hair and one decent canter. fair trade.",
    "cold hands, wet boots, still riding.",
    "long day. good horse. bad hair.",
    "feed, stalls, rides, repeat.",
    "barn dust and tired legs. normal.",
    "not glamorous. still my favorite kind of day.",
    "worked all day and still wanted one more ride.",
    "five horses later, somehow still learning.",
  ],
  confidence: [
    "quietly better.",
    "that felt earned.",
    "no extra caption needed.",
    "kept it simple.",
    "rideable matters.",
    "better is enough.",
    "soft. straight. useful.",
    "not loud. solid.",
  ],
};

const monthLabel = new Intl.DateTimeFormat("en-US", {
  month: "short",
  year: "numeric",
});

function makeEmptyCounts(): Record<PostType, number> {
  const counts = {} as Record<PostType, number>;

  postTypes.forEach((type) => {
    counts[type.value] = 0;
  });

  return counts;
}

function shiftPool<T>(arr: T[], offset: number): T[] {
  if (arr.length === 0) return [];

  const start = offset % arr.length;
  return [...arr.slice(start), ...arr.slice(0, start)];
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function removeBannedPhrases(value: string): string {
  return voiceProfile.bannedPhrases.reduce((cleaned, phrase) => {
    return cleaned.replace(new RegExp(escapeRegExp(phrase), "gi"), "");
  }, value);
}

function normalizeCaptionLine(value: string): string {
  return removeBannedPhrases(value).replace(/\s+/g, " ").trim();
}

function shortenLine(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value;

  const cut = value.slice(0, maxLength).trimEnd();
  const lastSpace = cut.lastIndexOf(" ");
  const safeCut = lastSpace > 40 ? cut.slice(0, lastSpace) : cut;
  return `${safeCut}...`;
}

function limitCaptionLines(lines: string[], maxLines: number): string {
  return lines.filter(Boolean).slice(0, maxLines).join("\n");
}

function buildCaption(base: string, description: string, type: PostType): string {
  const rule = postTypeRules[type];
  const baseLine = normalizeCaptionLine(base);
  const desc = normalizeCaptionLine(description);

  if (!desc) return baseLine;

  const detailLine = type === "confidence" ? shortenLine(desc, 82) : shortenLine(desc, 138);
  return limitCaptionLines([baseLine, detailLine], rule.maxLines);
}

function currentMonthKey(date = new Date()): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function formatMonth(monthKey: string): string {
  return monthLabel.format(new Date(`${monthKey}-01T12:00:00`));
}

async function writeClipboard(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.top = "-1000px";
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand("copy");
  textarea.remove();
}

function VoiceProfileCard({ postType, activeRule }: { postType: PostType; activeRule: PostTypeRule }) {
  const knobEntries = Object.entries(voiceProfile.knobs).filter(([key]) => {
    return ["confidence", "humor", "softness", "maturity", "polish", "detail", "length"].includes(key);
  });

  return (
    <Card className="rounded-lg border-stone-200 shadow-sm">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">voice profile</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        <div className="flex flex-wrap gap-2">
          {voiceProfile.coreTone.slice(0, 7).map((tone) => (
            <Badge key={tone} variant="secondary" className="rounded-full bg-stone-100 text-stone-700">
              {tone}
            </Badge>
          ))}
        </div>

        <div className="grid gap-3">
          <div className="rounded-md border border-stone-200 bg-white p-3">
            <div className="mb-2 text-xs font-semibold uppercase tracking-[0.16em] text-stone-500">{postType}</div>
            <div className="flex flex-col gap-1 text-sm text-stone-700">
              {activeRule.leadWith.map((item) => (
                <div key={item}>lead: {item}</div>
              ))}
            </div>
          </div>

          <div className="rounded-md border border-stone-200 bg-white p-3">
            <div className="mb-2 text-xs font-semibold uppercase tracking-[0.16em] text-stone-500">keep out</div>
            <div className="flex flex-wrap gap-2">
              {[...activeRule.avoid.slice(0, 4), ...voiceProfile.avoid.slice(0, 3)].map((item) => (
                <Badge key={item} variant="outline" className="rounded-full border-stone-300 text-stone-600">
                  {item}
                </Badge>
              ))}
            </div>
          </div>
        </div>

        <div className="rounded-md bg-stone-100 p-3">
          <div className="mb-2 text-xs font-semibold uppercase tracking-[0.16em] text-stone-500">settings</div>
          <div className="flex flex-wrap gap-2">
            {knobEntries.map(([key, value]) => (
              <Badge key={key} className="rounded-full bg-stone-900 text-white">
                {key}: {value}
              </Badge>
            ))}
          </div>
          <div className="mt-3 text-xs leading-5 text-stone-600">
            {voiceProfile.generationChecklist.should.slice(0, 3).join(" / ")}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

export default function EquestrianCaptionPrototypeApp() {
  const [screen, setScreen] = useState<Screen>("create");
  const [postType, setPostType] = useState<PostType>("what i see");
  const [description, setDescription] = useState("");
  const [photoUrl, setPhotoUrl] = useState("");
  const [photoName, setPhotoName] = useState("");
  const [generationRound, setGenerationRound] = useState(0);
  const [selectedCaption, setSelectedCaption] = useState("");
  const [savedPosts, setSavedPosts] = useState<SavedPost[]>([]);
  const [copiedId, setCopiedId] = useState("");
  const [generated, setGenerated] = useState<GeneratedCaption[]>([]);

  const monthKey = currentMonthKey();
  const currentTypeMeta = postTypes.find((type) => type.value === postType) ?? postTypes[0];
  const activeRule = postTypeRules[postType];

  const currentMonthCounts = useMemo(() => {
    const counts = makeEmptyCounts();

    savedPosts.forEach((post) => {
      if (post.monthKey === monthKey) {
        counts[post.postType] += 1;
      }
    });

    return counts;
  }, [monthKey, savedPosts]);

  const dashboardRows = useMemo<DashboardRow[]>(() => {
    const months = Array.from(new Set(savedPosts.map((post) => post.monthKey))).sort().reverse();
    if (months.length === 0) months.push(monthKey);

    return months.map((rowMonthKey) => {
      const counts = makeEmptyCounts();

      savedPosts.forEach((post) => {
        if (post.monthKey === rowMonthKey) {
          counts[post.postType] += 1;
        }
      });

      const total = postTypes.reduce((sum, type) => sum + counts[type.value], 0);
      const expected = postTypes.reduce((sum, type) => sum + type.expected, 0);

      return {
        monthKey: rowMonthKey,
        counts,
        total,
        expected,
      };
    });
  }, [monthKey, savedPosts]);

  function handleFileChange(event: React.ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (!file) return;

    setPhotoName(file.name);

    const reader = new FileReader();
    reader.onload = () => setPhotoUrl(typeof reader.result === "string" ? reader.result : "");
    reader.readAsDataURL(file);
  }

  function generateCaptions(nextRound = generationRound) {
    const pool = shiftPool(starterPools[postType], nextRound * 4);
    const nextCaptions = pool.slice(0, 4).map((base, index) => ({
      id: `${postType}-${nextRound}-${index}`,
      text: buildCaption(base, description, postType),
    }));

    setGenerated(nextCaptions);
    setSelectedCaption("");
  }

  function refreshCaptions() {
    const nextRound = generationRound + 1;
    setGenerationRound(nextRound);
    generateCaptions(nextRound);
  }

  function resetCaptionsForType(nextPostType: PostType) {
    setPostType(nextPostType);
    setGenerationRound(0);
    setGenerated([]);
    setSelectedCaption("");
  }

  function savePost() {
    if (!selectedCaption) return;

    const entry: SavedPost = {
      id: String(Date.now()),
      postType,
      description,
      caption: selectedCaption,
      photoName,
      photoUrl,
      createdAt: new Date().toISOString(),
      monthKey,
    };

    setSavedPosts((prev) => [entry, ...prev]);
    setScreen("log");
  }

  async function copyCaption(text: string, id: string) {
    try {
      await writeClipboard(text);
      setCopiedId(id);
      window.setTimeout(() => setCopiedId(""), 1500);
    } catch (error) {
      console.error("Unable to copy caption", error);
    }
  }

  const screenTitle = screen === "create" ? "Create" : screen === "log" ? "Log" : "Dashboard";

  return (
    <div className="page">
      <div className="app">
        <header className="app-header">
          <button type="button" className="header-back is-invisible">
            Back
          </button>
          <h1 className="header-title">horse post builder / {screenTitle}</h1>
          <button
            type="button"
            className={screen === "create" ? "header-action" : "header-action is-invisible"}
            onClick={() => {
              setGenerationRound(0);
              generateCaptions(0);
            }}
          >
            Gen
          </button>
        </header>

        <main className="app-main">
          <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="list-column">
            {screen === "create" ? (
              <div className="start-logo">
                <div className="start-logo-title">mobile caption generator</div>
                <div className="start-logo-subtitle">
                  Pick a type, add the image notes, generate four options, then save one to the log.
                </div>
              </div>
            ) : null}

        {screen === "create" ? (
          <div className="screen-stack">
            <Card className="rounded-lg border-stone-200 shadow-sm">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">post type</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid h-auto w-full grid-cols-2 gap-2">
                  {postTypes.map((type) => {
                    const done = currentMonthCounts[type.value];
                    const isActive = postType === type.value;

                    return (
                      <button
                        key={type.value}
                        type="button"
                        onClick={() => resetCaptionsForType(type.value)}
                        aria-pressed={isActive}
                        className={`h-auto rounded-md border px-3 py-3 text-left ${
                          isActive
                            ? "border-stone-900 bg-stone-900 text-white"
                            : "border-stone-200 bg-white text-stone-900"
                        }`}
                      >
                        <span className="block w-full">
                          <span className="block text-sm font-medium leading-5">{type.value}</span>
                          <span className="mt-1 block text-[11px] leading-4 opacity-75">
                            {done}/{type.expected} this month
                          </span>
                        </span>
                      </button>
                    );
                  })}
                </div>
              </CardContent>
            </Card>

            <VoiceProfileCard postType={postType} activeRule={activeRule} />

            <Card className="rounded-lg border-stone-200 shadow-sm">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">image + description</CardTitle>
              </CardHeader>
              <CardContent className="flex flex-col gap-4">
                <label
                  htmlFor="horse-photo"
                  className="flex cursor-pointer flex-col items-center justify-center rounded-lg border border-dashed border-stone-300 bg-stone-50 px-4 py-5 text-center"
                >
                  {photoUrl ? (
                    <img
                      src={photoUrl}
                      alt="Uploaded horse"
                      className="mb-3 aspect-[4/5] w-full rounded-md object-cover"
                    />
                  ) : (
                    <span className="mb-3 rounded-full bg-white p-4 shadow-sm">
                      <Camera className="h-6 w-6 text-stone-600" />
                    </span>
                  )}
                  <span className="text-sm font-medium">{photoName || "upload image"}</span>
                  <input id="horse-photo" type="file" accept="image/*" className="hidden" onChange={handleFileChange} />
                </label>

                <div className="flex flex-col gap-2">
                  <Label htmlFor="image-description">describe the image</Label>
                  <Textarea
                    id="image-description"
                    value={description}
                    onChange={(event) => setDescription(event.target.value)}
                    placeholder="what is happening, what you liked, what made it funny, soft, honest, or worth posting"
                    className="min-h-[110px] rounded-md"
                  />
                </div>

                <div className="rounded-md bg-stone-100 p-3 text-sm text-stone-700">
                  <div className="font-medium">expected this month</div>
                  <div className="mt-1">
                    {currentTypeMeta.expected} total for {postType}
                  </div>
                  <div className="mt-1">done so far: {currentMonthCounts[postType]}</div>
                </div>

                <Button
                  onClick={() => {
                    setGenerationRound(0);
                    generateCaptions(0);
                  }}
                  className="w-full rounded-md bg-stone-900 text-white hover:bg-stone-800"
                >
                  <Sparkles className="mr-2 h-4 w-4" />
                  generate 4 captions
                </Button>
              </CardContent>
            </Card>

            {generated.length > 0 ? (
              <Card className="rounded-lg border-stone-200 shadow-sm">
                <CardHeader className="pb-3">
                  <div className="flex items-center justify-between gap-3">
                    <CardTitle className="text-base">caption options</CardTitle>
                    <Button onClick={refreshCaptions} variant="secondary" className="rounded-md">
                      <RefreshCcw className="mr-2 h-4 w-4" />
                      refresh 4 more
                    </Button>
                  </div>
                </CardHeader>
                <CardContent className="flex flex-col gap-3">
                  <div className="flex snap-x gap-3 overflow-x-auto pb-1">
                    {generated.map((item) => {
                      const isSelected = selectedCaption === item.text;

                      return (
                        <button
                          key={item.id}
                          type="button"
                          onClick={() => setSelectedCaption(item.text)}
                          aria-pressed={isSelected}
                          className={`min-w-[86%] snap-center rounded-lg border p-4 text-left ${
                            isSelected
                              ? "border-stone-900 bg-stone-900 text-white"
                              : "border-stone-200 bg-white text-stone-900"
                          }`}
                        >
                          <span className="mb-3 flex items-center justify-between">
                            <Badge
                              className={`rounded-full ${
                                isSelected ? "bg-white text-stone-900" : "bg-stone-100 text-stone-700"
                              }`}
                            >
                              option
                            </Badge>
                            {isSelected ? <Check className="h-4 w-4" /> : null}
                          </span>
                          <span className="block whitespace-pre-wrap text-sm leading-6">{item.text}</span>
                        </button>
                      );
                    })}
                  </div>

                  <Button
                    disabled={!selectedCaption}
                    onClick={savePost}
                    className="w-full rounded-md bg-stone-900 text-white hover:bg-stone-800 disabled:opacity-50"
                  >
                    <Check className="mr-2 h-4 w-4" />
                    save selected caption
                  </Button>
                </CardContent>
              </Card>
            ) : null}
          </div>
        ) : null}

        {screen === "log" ? (
          <div className="grid gap-4">
            {savedPosts.length === 0 ? (
              <Card className="rounded-lg border-stone-200 shadow-sm">
                <CardContent className="py-10 text-center text-sm text-stone-600">No saved posts yet.</CardContent>
              </Card>
            ) : (
              savedPosts.map((post) => (
                <Card key={post.id} className="rounded-lg border-stone-200 shadow-sm">
                  <CardContent className="p-4">
                    <div className="mb-3 flex items-center justify-between gap-3">
                      <Badge className="rounded-full bg-stone-900 text-white">{post.postType}</Badge>
                      <div className="text-xs text-stone-500">{new Date(post.createdAt).toLocaleDateString()}</div>
                    </div>
                    <div className="grid grid-cols-[84px,1fr] gap-3">
                      <div className="overflow-hidden rounded-md bg-stone-100">
                        {post.photoUrl ? (
                          <img
                            src={post.photoUrl}
                            alt={post.photoName || "horse"}
                            className="h-24 w-full object-cover"
                          />
                        ) : (
                          <div className="flex h-24 items-center justify-center text-stone-400">
                            <ImageIcon className="h-5 w-5" />
                          </div>
                        )}
                      </div>
                      <div>
                        <p className="mb-2 whitespace-pre-wrap text-sm leading-6 text-stone-800">{post.caption}</p>
                        <Button onClick={() => copyCaption(post.caption, post.id)} variant="secondary" className="rounded-md">
                          <Copy className="mr-2 h-4 w-4" />
                          {copiedId === post.id ? "copied" : "copy caption"}
                        </Button>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))
            )}
          </div>
        ) : null}

        {screen === "dashboard" ? (
          <div className="grid gap-4">
            <Card className="rounded-lg border-stone-200 shadow-sm">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">month totals vs expected</CardTitle>
              </CardHeader>
              <CardContent className="flex flex-col gap-4">
                {dashboardRows.map((row) => (
                  <div key={row.monthKey} className="rounded-md border border-stone-200 p-4">
                    <div className="mb-3 flex items-center justify-between">
                      <div className="font-medium">{formatMonth(row.monthKey)}</div>
                      <div className="text-sm text-stone-500">
                        {row.total}/{row.expected}
                      </div>
                    </div>
                    <div className="flex flex-col gap-3">
                      {postTypes.map((type) => {
                        const done = row.counts[type.value];
                        const expected = type.expected;
                        const pct = Math.min(100, Math.round((done / expected) * 100)) || 0;

                        return (
                          <div key={type.value}>
                            <div className="mb-1 flex items-center justify-between text-sm">
                              <span>{type.value}</span>
                              <span className="text-stone-500">
                                {done}/{expected}
                              </span>
                            </div>
                            <div className="h-2 rounded-full bg-stone-200">
                              <div className="h-2 rounded-full bg-stone-900" style={{ width: `${pct}%` }} />
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                ))}
              </CardContent>
            </Card>
          </div>
        ) : null}
      </div>
    </div>
  );
}
