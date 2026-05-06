"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowLeft,
  ArrowRight,
  Camera,
  Check,
  Copy,
  Image as ImageIcon,
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

const createSteps = [
  { value: "postType", label: "Post" },
  { value: "tags", label: "Tags" },
  { value: "image", label: "Image" },
] as const;

type PostType = (typeof postTypes)[number]["value"];
type Screen = "start" | "create" | "logs" | "dashboard";
type CreateStep = (typeof createSteps)[number]["value"] | "captions";

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
  recurringSeries: string[];
  highlightedLines: string[];
  outputRules: string[];
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

type CaptionTagPurpose = "detail" | "tone";
type CaptionTagSource = "global" | "post-type";
type CaptionTagSelectedBehavior = "caption-angle" | "caption-tone" | "filter-only";

type CaptionTag = {
  id: string;
  label: string;
  line: string;
  purpose: CaptionTagPurpose;
  source: CaptionTagSource;
  aliases: string[];
  attributes: string[];
  appliesTo: PostType[];
  selectedBehavior: CaptionTagSelectedBehavior;
  priority: number;
};

type CaptionTagGroups = Record<CaptionTagPurpose, CaptionTag[]>;
type CaptionTagMeta = Partial<
  Pick<CaptionTag, "source" | "aliases" | "attributes" | "appliesTo" | "selectedBehavior" | "priority">
>;

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
    "improves what she sits on",
    "no spotlight needed",
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
    "one real detail only",
  ],
  avoid: [
    "cheesy horse-girl language",
    "adult-sounding language",
    "adult coach tone",
    "influencer language",
    "bragging",
    "fake humility",
    "bitterness",
    "excuses",
    "martyr energy",
    "motivational quote energy",
    "stale stock caption language",
    "anything that sounds generated",
  ],
  bannedPhrases: [
    "$300k horse kids",
    "expensive horse kids",
    "so proud",
    "grateful beyond words",
    "grateful for this journey",
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
  recurringSeries: [
    "what this horse taught me",
    "how i helped bring this horse along",
    "not a ribbon round, still worth posting",
  ],
  highlightedLines: [
    "ears up for once.",
    "this horse stays in my camera roll.",
    "not a riding post just a cute one.",
    "another photo i didn't need but took anyway.",
    "no reason just him.",
    "not a ribbon round. still worth posting.",
    "spent the whole day making other people's horses go nicely.",
  ],
  outputRules: [
    "generate 4 caption options per request",
    "each caption should be 1 to 3 short lines",
    "at least 1 option should use a Lainey-highlight line",
    "never mention expensive-horse kids",
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
      "include at least one highlighted Lainey line in each generated set",
    ],
    shouldNot: [
      "sound like a brand",
      "sound like a trainer report",
      "sound fake-deep",
      "sound bitter",
      "make excuses",
      "sound like a martyr",
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
    "recurring series": [
      "what this horse taught me",
      "how i helped bring this horse along",
      "not a ribbon round, still worth posting",
    ],
    "highlighted lainey lines": [
      "ears up for once.",
      "this horse stays in my camera roll.",
      "not a riding post just a cute one.",
      "another photo i didn't need but took anyway.",
      "no reason just him.",
      "spent the whole day making other people's horses go nicely.",
    ],
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
      "line",
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
      "he was finally waiting for me to the base",
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
    endingStyle: ["short", "understated", "almost quiet", "that's the win"],
    variationKnobs: {
      observationSharpness: ["low", "medium", "high"],
      horseSpecificDetail: ["low", "medium", "high"],
      softness: ["low", "medium", "high"],
      humor: ["off", "light", "medium"],
    },
    manualPrompts: [
      "favorite opening words",
      "details i use a lot: waiting to the base, organized canter, softer change, between hand and leg",
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
      "meme slang",
      "too many jokes",
    ],
    endingStyle: ["quick", "punchy", "deadpan if possible"],
    variationKnobs: {
      humor: ["low", "medium", "high"],
      sarcasm: ["off", "light", "medium"],
      affection: ["low", "medium", "high"],
      chaosLevel: ["low", "medium", "high"],
    },
    manualPrompts: [
      "i was not ___ today but she still ___",
      "she asked me to ___",
      "not every ride is ___",
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
    structure: ["focused on ___", "kept it ___", "by the end ___"],
    emotionalRange: ["steady", "productive", "confident", "practical", "not showy"],
    allowedQualities: ["useful", "horse-first", "clear", "grounded", "quietly proud"],
    avoid: [
      "sounding like a lesson note from an adult trainer",
      "adult coach tone",
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
    structure: ["what did not happen", "what still mattered"],
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
      "not a ribbon round. still worth posting.",
      "didn't get the prize, got the answer.",
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
    structure: ["one real sentence", "no complaining"],
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
      "martyr energy",
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
      "spent the whole day making other people's horses go nicely.",
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
    structure: ["1 to 3 words", "maybe two short phrases", "no explanation unless needed"],
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
    "he was finally waiting for me to the base.",
    "that mattered more than the placing.",
    "the canter stayed organized the whole trip.",
    "she gave me a much softer change today.",
    "he finally stayed between my hand and leg.",
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
    "i was not simple today but she still stayed quiet.",
    "she asked me to do my job today.",
    "not every ride is fancy.",
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
    "focused on rhythm.",
    "kept it quiet.",
    "by the end, he was actually waiting.",
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
    "not a ribbon round. still worth posting.",
    "didn't get the prize, got the answer.",
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
    "spent the whole day making other people's horses go nicely.",
    "early feed, tired legs, still riding.",
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
    "useful.",
    "better.",
    "rideable.",
    "earned.",
    "not fancy. effective.",
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

const highlightedLinesByPostType: Record<PostType, string[]> = {
  "what i see": [
    "ears up for once.",
    "this horse stays in my camera roll.",
    "he was finally waiting for me to the base.",
  ],
  "what the horse sees": [
    "ears up for once.",
    "no reason just him.",
    "another photo i didn't need but took anyway.",
  ],
  "what we did": [
    "how i helped bring this horse along.",
    "what this horse taught me.",
    "spent the whole day making other people's horses go nicely.",
  ],
  "what we almost did": [
    "not a ribbon round. still worth posting.",
    "didn't get the prize, got the answer.",
  ],
  reality: [
    "spent the whole day making other people's horses go nicely.",
    "not a riding post just a cute one.",
    "another photo i didn't need but took anyway.",
  ],
  confidence: [
    "useful.",
    "better.",
    "rideable.",
    "earned.",
    "not fancy. effective.",
  ],
};

function buildTagAliases(label: string, line: string, aliases: string[] = []): string[] {
  const normalizedLine = normalizeTagLabel(line.replace(/\.$/, ""));
  const values = [
    label,
    label.replaceAll("-", " "),
    normalizedLine,
    ...aliases,
  ].filter(Boolean);

  return Array.from(new Set(values.map((value) => normalizeTagLabel(value))));
}

function buildTagAttributes(label: string, attributes: string[] = []): string[] {
  const labelParts = normalizeTagLabel(label).split(" ").filter(Boolean);
  return Array.from(new Set([...labelParts, ...attributes.map((value) => normalizeTagLabel(value))]));
}

function makeCaptionTag(
  id: string,
  label: string,
  line: string,
  purpose: CaptionTagPurpose,
  meta: CaptionTagMeta = {},
): CaptionTag {
  return {
    id,
    label,
    line,
    purpose,
    source: meta.source ?? "post-type",
    aliases: buildTagAliases(label, line, meta.aliases),
    attributes: buildTagAttributes(label, meta.attributes),
    appliesTo: meta.appliesTo ?? [],
    selectedBehavior: meta.selectedBehavior ?? (purpose === "detail" ? "caption-angle" : "caption-tone"),
    priority: meta.priority ?? (meta.source === "global" ? 10 : 30),
  };
}

function detailTag(id: string, label: string, line: string, meta?: CaptionTagMeta): CaptionTag {
  return makeCaptionTag(id, label, line, "detail", meta);
}

function toneTag(id: string, label: string, line: string, meta?: CaptionTagMeta): CaptionTag {
  return makeCaptionTag(id, label, line, "tone", meta);
}

const globalDetailPillLabels = [
  "base",
  "canter",
  "change",
  "line",
  "rhythm",
  "softness",
  "rideability",
  "what-the-horse-dealt-with",
  "what-you-asked",
  "how-it-answered",
  "one-thing",
  "one-result",
  "answer",
  "quiet-ride",
  "better-finish",
  "ribbon",
  "clean-round",
  "perfect-trip",
  "big-result",
  "improvement",
  "feel",
  "trust",
  "boots",
  "dust",
  "tired-legs",
  "multiple-rides",
  "late-day",
  "cold-hands",
  "kids",
  "progress",
  "working-student",
  "junior-rider",
  "hunter-equitation",
  "horsewoman",
  "horsemanship",
  "early-mornings",
  "long-days",
  "barn-ready",
  "show-week",
  "ring-ready",
  "on-time",
  "schedule-smart",
  "order-of-go",
  "in-gate",
  "groom-alerts",
  "trainer-view",
  "rider-first",
  "packing-checklists",
  "tack-ready",
  "team-first",
  "pony-mentor",
  "confidence-builder",
  "horse-development",
  "fast",
  "sms",
  "mobile",
  "modular",
  "barn-board",
  "routines",
  "structure",
  "preparation",
  "safety",
  "barn-to-ring",
  "timing",
  "execution",
  "last-minute",
  "readiness",
  "coordination",
  "communication",
  "overlay",
  "next-up",
  "share",
  "fast-answers",
  "workflow",
  "review",
  "alerts",
  "less-stress",
  "triggers",
  "targeting",
  "prep",
  "walk",
  "two-way",
  "realtime",
  "query",
  "now-next",
  "templates",
  "inventory",
  "check-off",
  "ringwaze",
  "crowd",
  "community",
  "signals",
  "efficiency",
  "reminders",
  "deadlines",
  "care",
  "per-horse",
  "repeating",
  "notes",
  "stay-ahead",
  "shipping",
  "travel",
  "papers",
  "contacts",
  "eta",
  "feed",
  "am-pm",
  "log",
  "turnout",
  "mornings",
  "status",
  "restrictions",
  "lessons",
  "less-texting",
  "calendar",
  "assignments",
  "schooling",
  "weekly-plan",
  "horse-care",
  "flat",
  "jump",
  "hack",
  "rest",
  "plan",
  "balance",
  "show-board",
  "alignment",
  "day-view",
];

const globalTonePillLabels = [
  "confident",
  "horse-first",
  "calm-under-pressure",
  "quiet-leadership",
  "consistent",
  "accountable",
  "detail-oriented",
  "reliable",
  "low-drama",
  "simple",
  "focused",
  "no-fluff",
  "understated",
  "dependable",
  "credible",
  "observant",
  "honest",
  "casual",
  "funny",
  "dry",
  "soft",
  "team-first",
];

const defaultDetailLabelsByPostType: Record<PostType, string[]> = {
  "what i see": ["base", "canter", "change", "line", "rhythm", "softness", "rideability", "feel", "balance"],
  "what the horse sees": [
    "what-the-horse-dealt-with",
    "what-you-asked",
    "how-it-answered",
    "one-thing",
    "timing",
  ],
  "what we did": [
    "what-you-asked",
    "how-it-answered",
    "answer",
    "quiet-ride",
    "better-finish",
    "improvement",
    "progress",
    "horse-development",
  ],
  "what we almost did": [
    "one-result",
    "ribbon",
    "clean-round",
    "perfect-trip",
    "big-result",
    "improvement",
    "better-finish",
    "answer",
  ],
  reality: [
    "boots",
    "dust",
    "tired-legs",
    "multiple-rides",
    "late-day",
    "cold-hands",
    "working-student",
    "early-mornings",
    "long-days",
    "show-week",
  ],
  confidence: [
    "one-thing",
    "one-result",
    "feel",
    "trust",
    "rideability",
    "confidence-builder",
    "execution",
    "readiness",
    "balance",
  ],
};

function normalizeTagLabel(label: string): string {
  return label.toLowerCase().replace(/[-\s]+/g, " ").trim();
}

function getDefaultDetailPostTypes(label: string): PostType[] {
  const normalizedLabel = normalizeTagLabel(label);

  return postTypes
    .filter((postTypeItem) => {
      return defaultDetailLabelsByPostType[postTypeItem.value].some((defaultLabel) => {
        return normalizeTagLabel(defaultLabel) === normalizedLabel;
      });
    })
    .map((postTypeItem) => postTypeItem.value);
}

function detailTagFromLabel(prefix: string, label: string): CaptionTag {
  const id = label.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "");
  return detailTag(`${prefix}-global-detail-${id}`, label, `${label.replaceAll("-", " ")}.`, {
    source: "global",
    appliesTo: getDefaultDetailPostTypes(label),
    attributes: ["global-detail"],
    selectedBehavior: "caption-angle",
  });
}

function toneTagFromLabel(prefix: string, label: string): CaptionTag {
  const id = label.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "");
  return toneTag(`${prefix}-global-tone-${id}`, label, `${label.replaceAll("-", " ")}.`, {
    source: "global",
    appliesTo: postTypes.map((postTypeItem) => postTypeItem.value),
    attributes: ["global-tone"],
    selectedBehavior: "caption-tone",
  });
}

function mergeDetailTags(prefix: string, localTags: CaptionTag[]): CaptionTag[] {
  const globalTags = globalDetailPillLabels.map((label) => detailTagFromLabel(prefix, label));
  const globalLabels = new Set(globalTags.map((tag) => normalizeTagLabel(tag.label)));

  return [...globalTags, ...localTags.filter((tag) => !globalLabels.has(normalizeTagLabel(tag.label)))];
}

function mergeToneTags(prefix: string, localTags: CaptionTag[]): CaptionTag[] {
  const globalTags = globalTonePillLabels.map((label) => toneTagFromLabel(prefix, label));
  const globalLabels = new Set(globalTags.map((tag) => normalizeTagLabel(tag.label)));

  return [...globalTags, ...localTags.filter((tag) => !globalLabels.has(normalizeTagLabel(tag.label)))];
}

const tagOptionsByPostType: Record<PostType, CaptionTagGroups> = {
  "what i see": {
    detail: mergeDetailTags("wis", [
      detailTag("wis-waiting-base", "waiting to base", "waited to the base."),
      detailTag("wis-organized-canter", "organized canter", "organized canter."),
      detailTag("wis-softer-change", "softer change", "softer change."),
      detailTag("wis-straightness", "straightness", "stayed straighter."),
      detailTag("wis-rhythm", "rhythm", "rhythm stayed normal."),
      detailTag("wis-relaxed-mouth", "relaxed mouth", "less noise in the bridle."),
      detailTag("wis-better-distance", "better distance", "the distance showed up."),
      detailTag("wis-quieter-hand", "quieter hand", "quieter in my hand."),
      detailTag("wis-stayed-balanced", "stayed balanced", "stayed balanced after the turn."),
      detailTag("wis-inside-leg", "inside leg", "actually listened to my inside leg."),
      detailTag("wis-landed-straighter", "landed straighter", "landed straighter."),
      detailTag("wis-waited-after", "waited after fence", "waited after the jump."),
    ]),
    tone: mergeToneTags("wis", [
      toneTag("wis-quietly-pleased", "quietly pleased", "small win."),
      toneTag("wis-dry", "dry", "finally."),
      toneTag("wis-matter-of-fact", "matter-of-fact", "that was the point."),
      toneTag("wis-soft", "soft", "i'll take it."),
      toneTag("wis-sharp-eye", "sharp eye", "worth noticing."),
      toneTag("wis-small-win", "small win", "small but useful."),
      toneTag("wis-calm", "calm", "quietly better."),
      toneTag("wis-teen-honest", "teen honest", "not fancy. just better."),
      toneTag("wis-no-drama", "no drama", "no big speech needed."),
      toneTag("wis-useful", "useful", "useful detail."),
    ]),
  },
  "what the horse sees": {
    detail: mergeDetailTags("whs", [
      detailTag("whs-opinion", "opinion", "had an opinion."),
      detailTag("whs-weird-timing", "weird timing", "timing was a choice."),
      detailTag("whs-tolerated-me", "tolerated me", "tolerated me anyway."),
      detailTag("whs-chaos", "chaos", "provided character development."),
      detailTag("whs-ears", "ears", "ears had their own plan."),
      detailTag("whs-side-eye", "side eye", "side eye was earned."),
      detailTag("whs-spooking", "spooking", "found something to inspect."),
      detailTag("whs-left-lead", "lead debate", "the lead was apparently debatable."),
      detailTag("whs-snack-agenda", "snack agenda", "snacks were still the main goal."),
      detailTag("whs-steering-thoughts", "steering thoughts", "steering was a group project."),
      detailTag("whs-rider-overthinking", "rider overthinking", "she was overthinking again."),
      detailTag("whs-jump-judgment", "jump judgment", "judged the jump and me."),
    ]),
    tone: mergeToneTags("whs", [
      toneTag("whs-deadpan", "deadpan", "fair enough."),
      toneTag("whs-sarcastic", "mildly sarcastic", "apparently."),
      toneTag("whs-playful", "playful", "i had notes."),
      toneTag("whs-low-affection", "low affection", "still tried."),
      toneTag("whs-judgy", "judgy", "valid concern."),
      toneTag("whs-silly", "silly", "normal behavior, sadly."),
      toneTag("whs-patient", "patient", "very patient of me."),
      toneTag("whs-not-impressed", "not impressed", "not my favorite idea."),
      toneTag("whs-tiny-chaos", "tiny chaos", "small chaos. tasteful."),
      toneTag("whs-soft-funny", "soft funny", "she meant well."),
    ]),
  },
  "what we did": {
    detail: mergeDetailTags("wwd", [
      detailTag("wwd-rhythm-first", "rhythm first", "rhythm first."),
      detailTag("wwd-softer", "softer", "softer by the end."),
      detailTag("wwd-rideable", "rideable", "more rideable."),
      detailTag("wwd-brought-along", "brought along", "a little more together."),
      detailTag("wwd-transitions", "transitions", "cleaner transitions."),
      detailTag("wwd-straight-lines", "straight lines", "straighter lines."),
      detailTag("wwd-quieter-canter", "quieter canter", "quieter canter."),
      detailTag("wwd-wait-after-fence", "wait after fence", "waited after the fence."),
      detailTag("wwd-balance", "balance", "better balance."),
      detailTag("wwd-forward-no-run", "forward no run", "forward without running."),
      detailTag("wwd-lower-neck", "lower neck", "lower neck, softer back."),
      detailTag("wwd-simple-changes", "simple changes", "made the changes simpler."),
    ]),
    tone: mergeToneTags("wwd", [
      toneTag("wwd-practical", "practical", "useful day."),
      toneTag("wwd-quiet-pride", "quiet pride", "earned that."),
      toneTag("wwd-grounded", "grounded", "nothing dramatic."),
      toneTag("wwd-warm", "warm", "good little step."),
      toneTag("wwd-productive", "productive", "productive enough."),
      toneTag("wwd-horse-first", "horse first", "good answer for him."),
      toneTag("wwd-steady", "steady", "steady work."),
      toneTag("wwd-not-showy", "not showy", "not showy. helpful."),
      toneTag("wwd-useful", "useful", "useful ride."),
      toneTag("wwd-earned", "earned", "earned the better feel."),
    ]),
  },
  "what we almost did": {
    detail: mergeDetailTags("wwad", [
      detailTag("wwad-one-rail", "one rail", "one rail."),
      detailTag("wwad-no-ribbon", "no ribbon", "no ribbon."),
      detailTag("wwad-better-answer", "better answer", "better answer after."),
      detailTag("wwad-useful-miss", "useful miss", "useful miss."),
      detailTag("wwad-late-change", "late change", "late change."),
      detailTag("wwad-missed-distance", "missed distance", "missed the distance."),
      detailTag("wwad-got-close", "got close", "got close."),
      detailTag("wwad-stayed-rideable", "stayed rideable", "stayed rideable after."),
      detailTag("wwad-recovered", "recovered", "recovered well."),
      detailTag("wwad-almost-there", "almost there", "almost there."),
      detailTag("wwad-learned-spot", "learned spot", "learned where it left."),
      detailTag("wwad-better-finish", "better finish", "better finish."),
    ]),
    tone: mergeToneTags("wwad", [
      toneTag("wwad-calm", "calm", "still worth posting."),
      toneTag("wwad-tough", "tough", "kept riding."),
      toneTag("wwad-dry", "dry", "annoying. helpful."),
      toneTag("wwad-honest", "honest", "not wasted."),
      toneTag("wwad-no-excuses", "no excuses", "no excuses."),
      toneTag("wwad-slightly-annoyed", "slightly annoyed", "slightly annoying."),
      toneTag("wwad-steady", "steady", "kept it together."),
      toneTag("wwad-still-useful", "still useful", "still useful."),
      toneTag("wwad-perspective", "perspective", "not the whole story."),
      toneTag("wwad-not-wasted", "not wasted", "not wasted."),
    ]),
  },
  reality: {
    detail: mergeDetailTags("reality", [
      detailTag("reality-long-day", "long barn day", "long barn day."),
      detailTag("reality-chores", "chores first", "chores first."),
      detailTag("reality-tired-legs", "tired legs", "tired legs."),
      detailTag("reality-weather", "weather", "barn weather."),
      detailTag("reality-wet-boots", "wet boots", "wet boots."),
      detailTag("reality-hay-hair", "hay hair", "hay in my hair."),
      detailTag("reality-feed-then-ride", "feed then ride", "feed, then ride."),
      detailTag("reality-late-ride", "late ride", "late ride."),
      detailTag("reality-dusty-tack", "dusty tack", "dusty tack."),
      detailTag("reality-cold-hands", "cold hands", "cold hands."),
      detailTag("reality-five-horses", "five horses", "five horses later."),
      detailTag("reality-one-more", "one more ride", "one more ride."),
    ]),
    tone: mergeToneTags("reality", [
      toneTag("reality-dry", "dry", "normal."),
      toneTag("reality-funny", "funny", "fair trade."),
      toneTag("reality-matter-of-fact", "matter-of-fact", "still rode."),
      toneTag("reality-soft", "soft", "worth it."),
      toneTag("reality-tired", "tired", "tired but fine."),
      toneTag("reality-honest", "honest", "real barn day."),
      toneTag("reality-real", "real", "that is the day."),
      toneTag("reality-low-drama", "low drama", "no speech."),
      toneTag("reality-normal", "normal", "normal day."),
      toneTag("reality-worth-it", "worth it", "still worth it."),
    ]),
  },
  confidence: {
    detail: mergeDetailTags("confidence", [
      detailTag("confidence-simple-win", "simple win", "simple win."),
      detailTag("confidence-strong-image", "strong image", "image says enough."),
      detailTag("confidence-rideable", "rideable", "rideable."),
      detailTag("confidence-earned-feel", "earned feel", "earned feel."),
      detailTag("confidence-soft-straight", "soft + straight", "soft and straight."),
      detailTag("confidence-no-extra", "no extra", "no extra."),
      detailTag("confidence-solid-canter", "solid canter", "solid canter."),
      detailTag("confidence-better-answer", "better answer", "better answer."),
      detailTag("confidence-clean-moment", "clean moment", "clean moment."),
      detailTag("confidence-useful-ride", "useful ride", "useful ride."),
      detailTag("confidence-quiet-photo", "quiet photo", "quiet photo."),
      detailTag("confidence-good-one", "good one", "good one."),
    ]),
    tone: mergeToneTags("confidence", [
      toneTag("confidence-final", "final", "enough said."),
      toneTag("confidence-understated", "understated", "quietly solid."),
      toneTag("confidence-sharper", "sharper", "useful."),
      toneTag("confidence-warm", "warm", "good one."),
      toneTag("confidence-blunt", "blunt", "better."),
      toneTag("confidence-quiet", "quiet", "quietly better."),
      toneTag("confidence-strong", "strong", "solid."),
      toneTag("confidence-simple", "simple", "simple enough."),
      toneTag("confidence-earned", "earned", "earned."),
      toneTag("confidence-no-speech", "no speech", "no speech needed."),
    ]),
  },
};

const monthLabel = new Intl.DateTimeFormat("en-US", {
  month: "short",
  year: "numeric",
});
const sessionTimeLabel = new Intl.DateTimeFormat("en-US", {
  hour: "numeric",
  minute: "2-digit",
});

const EMPTY_TAG_IDS: string[] = [];
const SESSION_STORAGE_KEY = "lainey-caption-builder-session-v1";
const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const TAG_HINT_STOP_WORDS = new Set([
  "and",
  "for",
  "the",
  "with",
  "that",
  "this",
  "you",
  "what",
  "how",
  "one",
  "was",
  "were",
]);

type PersistedSessionState = {
  postType: PostType;
  description: string;
  photoUrl: string;
  photoName: string;
  generationRound: number;
  selectedCaption: string;
  savedPosts: SavedPost[];
  showVoiceProfile: boolean;
  selectedTagIdsByType: Partial<Record<PostType, string[]>>;
};

type PersistedSession = {
  version: 1;
  savedAt: string;
  expiresAt: string;
  state: PersistedSessionState;
};

function readStoredSession(): PersistedSession | null {
  if (typeof window === "undefined") return null;

  try {
    const raw = window.localStorage.getItem(SESSION_STORAGE_KEY);
    if (!raw) return null;

    const parsed = JSON.parse(raw) as PersistedSession;
    const expiresAtMs = Date.parse(parsed.expiresAt);
    if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
      window.localStorage.removeItem(SESSION_STORAGE_KEY);
      return null;
    }

    return parsed;
  } catch (error) {
    console.error("Unable to read saved caption session", error);
    return null;
  }
}

function writeStoredSession(state: PersistedSessionState): PersistedSession | null {
  if (typeof window === "undefined") return null;

  const savedAt = new Date();
  const payload: PersistedSession = {
    version: 1,
    savedAt: savedAt.toISOString(),
    expiresAt: new Date(savedAt.getTime() + SESSION_TTL_MS).toISOString(),
    state,
  };

  try {
    window.localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(payload));
    return payload;
  } catch (error) {
    console.error("Unable to save caption session", error);
    return null;
  }
}

function clearStoredSession() {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(SESSION_STORAGE_KEY);
}

function formatSessionTime(value: string) {
  if (!value) return "--";

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";

  return sessionTimeLabel.format(date);
}

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

function dedupeCaptionLines(lines: string[]): string[] {
  const seen = new Set<string>();
  const uniqueLines: string[] = [];

  lines.forEach((line) => {
    const key = normalizeCaptionLine(line).toLowerCase();
    if (!key || seen.has(key)) return;

    seen.add(key);
    uniqueLines.push(line);
  });

  return uniqueLines;
}

function getTagsForPostType(type: PostType): CaptionTag[] {
  const groups = tagOptionsByPostType[type];
  return [...groups.detail, ...groups.tone];
}

function getSelectedTagsForPostType(type: PostType, selectedTagIds: string[]): CaptionTag[] {
  const selectedIds = new Set(selectedTagIds);
  return getTagsForPostType(type).filter((tag) => selectedIds.has(tag.id));
}

function getTagHintTerms(selectedTags: CaptionTag[]): string[] {
  const terms = selectedTags
    .filter((tag) => tag.selectedBehavior !== "filter-only")
    .sort((a, b) => b.priority - a.priority)
    .flatMap((tag) => {
      return [tag.label, tag.line, ...tag.aliases, ...tag.attributes].flatMap((value) => {
      return normalizeTagLabel(value)
        .split(" ")
        .filter((term) => term.length > 2 && !TAG_HINT_STOP_WORDS.has(term));
    });
    });

  return Array.from(new Set(terms));
}

function scoreStarterLineWithHints(base: string, hintTerms: string[]): number {
  const normalizedBase = normalizeTagLabel(base);

  return hintTerms.reduce((score, term) => {
    return normalizedBase.includes(term) ? score + 1 : score;
  }, 0);
}

function prioritizeStarterPoolByHints(pool: string[], selectedTags: CaptionTag[]): string[] {
  const hintTerms = getTagHintTerms(selectedTags);
  if (hintTerms.length === 0) return pool;

  return pool
    .map((line, index) => ({
      index,
      line,
      score: scoreStarterLineWithHints(line, hintTerms),
    }))
    .sort((a, b) => b.score - a.score || a.index - b.index)
    .map((item) => item.line);
}

function buildCaption(base: string, description: string, type: PostType): string {
  const rule = postTypeRules[type];
  const baseLine = normalizeCaptionLine(base);
  const desc = normalizeCaptionLine(description);

  if (!desc) return baseLine;

  const maxSupportingLength = type === "confidence" ? 82 : 138;
  const supportingLine = shortenLine(desc, maxSupportingLength);

  return limitCaptionLines(dedupeCaptionLines([baseLine, supportingLine]), rule.maxLines);
}

function pickHighlightedLine(type: PostType, round: number): string {
  const lines = highlightedLinesByPostType[type];
  return lines[round % lines.length];
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

function TagsCard({
  postType,
  selectedTagIds,
  onToggleTag,
}: {
  postType: PostType;
  selectedTagIds: string[];
  onToggleTag: (tagId: string) => void;
}) {
  const tagGroups = tagOptionsByPostType[postType];

  return (
    <Card className="rounded-lg border-stone-200 shadow-sm">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">tags</CardTitle>
      </CardHeader>
      <CardContent className="tag-card-content">
        {(["detail", "tone"] as const).map((purpose) => (
          <div key={purpose} className="tag-purpose-group">
            <div className="tag-purpose-label">{purpose}</div>
            <div className="tag-pill-wrap" aria-label={`${purpose} tags`}>
              {tagGroups[purpose].map((tag) => {
                const isActive = selectedTagIds.includes(tag.id);

                return (
                  <button
                    key={tag.id}
                    type="button"
                    onClick={() => onToggleTag(tag.id)}
                    aria-pressed={isActive}
                    data-purpose={purpose}
                    className={`tag-pill ${isActive ? "is-active" : ""}`}
                  >
                    {tag.label}
                  </button>
                );
              })}
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function CreateProgress({ step }: { step: CreateStep }) {
  const stepIndex = createSteps.findIndex((item) => item.value === step);
  const progress = ((stepIndex + 1) / createSteps.length) * 100;

  return (
    <div className="create-progress" aria-label={`Create step ${stepIndex + 1} of ${createSteps.length}`}>
      <div className="create-progress-track">
        <div className="create-progress-fill" style={{ width: `${progress}%` }} />
      </div>
      <div className="create-progress-labels">
        {createSteps.map((item, index) => (
          <span
            key={item.value}
            className={index <= stepIndex ? "is-active" : ""}
          >
            {index + 1}. {item.label}
          </span>
        ))}
      </div>
    </div>
  );
}

export default function EquestrianCaptionPrototypeApp() {
  const [initialSession] = useState(() => readStoredSession());
  const [screen, setScreen] = useState<Screen>("start");
  const [createStep, setCreateStep] = useState<CreateStep>("postType");
  const [postType, setPostType] = useState<PostType>(initialSession?.state.postType ?? "what i see");
  const [description, setDescription] = useState(initialSession?.state.description ?? "");
  const [photoUrl, setPhotoUrl] = useState(initialSession?.state.photoUrl ?? "");
  const [photoName, setPhotoName] = useState(initialSession?.state.photoName ?? "");
  const [generationRound, setGenerationRound] = useState(initialSession?.state.generationRound ?? 0);
  const [selectedCaption, setSelectedCaption] = useState(initialSession?.state.selectedCaption ?? "");
  const [savedPosts, setSavedPosts] = useState<SavedPost[]>(initialSession?.state.savedPosts ?? []);
  const [copiedId, setCopiedId] = useState("");
  const [generated, setGenerated] = useState<GeneratedCaption[]>([]);
  const [showVoiceProfile, setShowVoiceProfile] = useState(initialSession?.state.showVoiceProfile ?? false);
  const [selectedTagIdsByType, setSelectedTagIdsByType] = useState<Partial<Record<PostType, string[]>>>(
    initialSession?.state.selectedTagIdsByType ?? {},
  );
  const [visibleCaptionIndex, setVisibleCaptionIndex] = useState(0);
  const [lastSavedAt, setLastSavedAt] = useState(initialSession?.savedAt ?? "");
  const [expiresAt, setExpiresAt] = useState(initialSession?.expiresAt ?? "");
  const captionScrollRef = useRef<HTMLDivElement>(null);

  const monthKey = currentMonthKey();
  const activeRule = postTypeRules[postType];
  const createStepIndex = createSteps.findIndex((step) => step.value === createStep);
  const selectedTagIds = selectedTagIdsByType[postType] ?? EMPTY_TAG_IDS;
  const selectedTags = useMemo(() => {
    return getSelectedTagsForPostType(postType, selectedTagIds);
  }, [postType, selectedTagIds]);

  useEffect(() => {
    const savedSession = writeStoredSession({
      postType,
      description,
      photoUrl,
      photoName,
      generationRound,
      selectedCaption,
      savedPosts,
      showVoiceProfile,
      selectedTagIdsByType,
    });

    if (savedSession) {
      setLastSavedAt(savedSession.savedAt);
      setExpiresAt(savedSession.expiresAt);
    }
  }, [
    description,
    generationRound,
    photoName,
    photoUrl,
    postType,
    savedPosts,
    selectedCaption,
    selectedTagIdsByType,
    showVoiceProfile,
  ]);

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

  function scrollCaptionTo(index: number) {
    window.requestAnimationFrame(() => {
      const scrollContainer = captionScrollRef.current;
      const target = scrollContainer?.querySelector<HTMLElement>(`[data-caption-index="${index}"]`);
      target?.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
        inline: "center",
      });
    });
  }

  function showCaptionAt(index: number) {
    if (generated.length === 0) return;

    const nextIndex = Math.max(0, Math.min(index, generated.length - 1));
    setVisibleCaptionIndex(nextIndex);
    scrollCaptionTo(nextIndex);
  }

  function handleCaptionScroll() {
    const scrollContainer = captionScrollRef.current;
    const firstCard = scrollContainer?.querySelector<HTMLElement>(".caption-option-card");
    if (!scrollContainer || !firstCard || generated.length === 0) return;

    const styles = window.getComputedStyle(scrollContainer);
    const gap = Number.parseFloat(styles.columnGap || styles.gap || "0") || 0;
    const cardWidth = firstCard.getBoundingClientRect().width + gap;
    if (cardWidth <= 0) return;

    const nextIndex = Math.max(0, Math.min(Math.round(scrollContainer.scrollLeft / cardWidth), generated.length - 1));
    if (nextIndex !== visibleCaptionIndex) {
      setVisibleCaptionIndex(nextIndex);
    }
  }

  function generateCaptions(nextRound = generationRound) {
    const highlightedLine = pickHighlightedLine(postType, nextRound);
    const pool = prioritizeStarterPoolByHints(
      shiftPool(
        starterPools[postType].filter((line) => line !== highlightedLine),
        nextRound * 3,
      ),
      selectedTags,
    );
    const regularCaptions = pool.slice(0, 3).map((base, index) => ({
      id: `${postType}-${nextRound}-${index}`,
      text: buildCaption(base, description, postType),
    }));
    const highlightedCaption = {
      id: `${postType}-${nextRound}-highlight`,
      text: buildCaption(highlightedLine, "", postType),
    };

    setGenerated([...regularCaptions, highlightedCaption]);
    setSelectedCaption("");
    setVisibleCaptionIndex(0);
    setCreateStep("captions");
    scrollCaptionTo(0);
  }

  function refreshCaptions() {
    const nextRound = generationRound + 1;
    setGenerationRound(nextRound);
    generateCaptions(nextRound);
  }

  function selectPostType(nextPostType: PostType) {
    setPostType(nextPostType);
    setGenerationRound(0);
    setGenerated([]);
    setSelectedCaption("");
    setVisibleCaptionIndex(0);
  }

  function toggleTag(tagId: string) {
    setSelectedTagIdsByType((prev) => {
      const currentTagIds = prev[postType] ?? EMPTY_TAG_IDS;
      const nextTagIds = currentTagIds.includes(tagId)
        ? currentTagIds.filter((currentTagId) => currentTagId !== tagId)
        : [...currentTagIds, tagId];

      return {
        ...prev,
        [postType]: nextTagIds,
      };
    });
    setGenerationRound(0);
    setGenerated([]);
    setSelectedCaption("");
    setVisibleCaptionIndex(0);
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
    setScreen("logs");
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

  function startCreateFlow() {
    setScreen("create");
    setCreateStep("postType");
  }

  function restartSession() {
    clearStoredSession();
    setScreen("start");
    setCreateStep("postType");
    setPostType("what i see");
    setDescription("");
    setPhotoUrl("");
    setPhotoName("");
    setGenerationRound(0);
    setSelectedCaption("");
    setSavedPosts([]);
    setCopiedId("");
    setGenerated([]);
    setShowVoiceProfile(false);
    setSelectedTagIdsByType({});
    setVisibleCaptionIndex(0);
    setLastSavedAt("");
    setExpiresAt("");
  }

  function goBack() {
    if (screen !== "create") return;

    if (createStep === "captions") {
      setCreateStep("image");
      return;
    }

    if (createStepIndex <= 0) {
      setScreen("start");
      return;
    }

    setCreateStep(createSteps[createStepIndex - 1].value);
  }

  function goNext() {
    if (screen === "start") {
      startCreateFlow();
      return;
    }

    if (screen !== "create") return;

    if (createStep === "postType") {
      setCreateStep("tags");
      return;
    }

    if (createStep === "tags") {
      setCreateStep("image");
      return;
    }

    if (createStep === "captions") return;

    setGenerationRound(0);
    generateCaptions(0);
  }

  const createStepTitle =
    createStep === "postType"
      ? "Post Type"
      : createStep === "tags"
        ? "Tags"
        : createStep === "image"
          ? "Image + Description"
          : "Caption Options";
  const screenTitle =
    screen === "start"
      ? "Start"
      : screen === "create"
        ? createStepTitle
        : screen === "logs"
          ? "Logs"
          : "Dashboard";
  const headerActionLabel = screen === "create" && createStep === "image" ? "Gen" : "Next";
  const showHeaderAction = screen === "create" && createStep !== "captions";
  const showHeaderBack = screen === "create";
  const autosaveText = `Autosave: ON (device). Last save: ${formatSessionTime(lastSavedAt)}. Expires: ${formatSessionTime(expiresAt)}.`;
  const summaryIsActive = savedPosts.length > 0;
  const voiceProfileText = showVoiceProfile
    ? `on: teen, horse-smart, ${activeRule.maxLines}L max`
    : "off";

  return (
    <div className="page">
      <div className="app">
        <header className="app-header">
          <button
            type="button"
            className={showHeaderBack ? "header-back" : "header-back is-invisible"}
            onClick={goBack}
            aria-hidden={!showHeaderBack}
            tabIndex={showHeaderBack ? 0 : -1}
          >
            {showHeaderBack ? (
              <>
                <ArrowLeft className="h-4 w-4" />
                Back
              </>
            ) : null}
          </button>
          <h1 className="header-title">{screenTitle}</h1>
          <button
            type="button"
            className={showHeaderAction ? "header-action" : "header-action is-invisible"}
            onClick={goNext}
            aria-hidden={!showHeaderAction}
            tabIndex={showHeaderAction ? 0 : -1}
          >
            {showHeaderAction ? (
              <>
                {headerActionLabel}
                {headerActionLabel === "Next" ? <ArrowRight className="h-4 w-4" /> : null}
              </>
            ) : null}
          </button>
        </header>

        <main className="app-main">
          <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="list-column">
            {screen === "start" ? (
              <div className="screen-stack">
                <div className="start-logo">
                  <div className="start-logo-title">Horse Post Builder</div>
                  <div className="start-logo-subtitle">Quick horse captions, in her voice.</div>
                </div>

                <div className="start-action-list" aria-label="Start options">
                  <button type="button" className="row row--tap row--active" onClick={startCreateFlow}>
                    <span className="row-title">In-session</span>
                    <span className="start-status-dot is-on" aria-hidden="true" />
                  </button>
                  <button type="button" className="row row--tap" onClick={() => setScreen("dashboard")}>
                    <span className="row-title">Summary</span>
                    <span className={`start-status-dot ${summaryIsActive ? "is-on" : ""}`} aria-hidden="true" />
                  </button>
                  <button type="button" className="row row--tap" onClick={restartSession}>
                    <span className="row-title">Restart session</span>
                    <span className="start-status-dot" aria-hidden="true" />
                  </button>
                </div>

                <div className="start-meta">
                  <div>{autosaveText}</div>
                  <label className="voice-subtle-line" htmlFor="voice-profile-toggle">
                    <input
                      id="voice-profile-toggle"
                      type="checkbox"
                      checked={showVoiceProfile}
                      onChange={(event) => setShowVoiceProfile(event.target.checked)}
                      className="voice-subtle-input"
                    />
                    <span>Voice Profile</span>
                    <span>{voiceProfileText}</span>
                  </label>
                </div>
              </div>
            ) : null}

            {screen === "create" ? (
              <div className="screen-stack create-screen-stack">
                {createStep !== "captions" ? <CreateProgress step={createStep} /> : null}

                {createStep === "postType" ? (
                  <>
                    <Card className="post-type-card rounded-lg border-stone-200 shadow-sm">
                      <CardHeader className="pb-3">
                        <CardTitle className="text-base">post type</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="post-type-grid">
                          {postTypes.map((type) => {
                            const isActive = postType === type.value;

                            return (
                              <button
                                key={type.value}
                                type="button"
                                onClick={() => selectPostType(type.value)}
                                aria-pressed={isActive}
                                className={`post-type-button ${isActive ? "is-active" : ""}`}
                              >
                                <span>{type.value}</span>
                              </button>
                            );
                          })}
                        </div>
                      </CardContent>
                    </Card>
                    <div className="step-bottom-action">
                      <Button onClick={() => setCreateStep("tags")} className="w-full">
                        Next
                        <ArrowRight className="ml-2 h-4 w-4" />
                      </Button>
                    </div>
                  </>
                ) : null}

                {createStep === "tags" ? (
                  <>
                    <TagsCard postType={postType} selectedTagIds={selectedTagIds} onToggleTag={toggleTag} />
                    <div className="step-bottom-action">
                      <Button onClick={() => setCreateStep("image")} className="w-full">
                        Next
                        <ArrowRight className="ml-2 h-4 w-4" />
                      </Button>
                    </div>
                  </>
                ) : null}

                {createStep === "image" ? (
                  <>
                    <Card className="rounded-lg border-stone-200 shadow-sm">
                      <CardHeader className="pb-3">
                        <CardTitle className="text-base">image + description</CardTitle>
                      </CardHeader>
                      <CardContent className="flex flex-col gap-4">
                        <label
                          htmlFor="horse-photo"
                          className="photo-drop flex cursor-pointer flex-col items-center justify-center px-4 py-5 text-center"
                        >
                          {photoUrl ? (
                            <img
                              src={photoUrl}
                              alt="Uploaded horse"
                              className="mb-3 aspect-[4/5] w-full rounded-md object-cover"
                            />
                          ) : (
                            <span className="photo-icon mb-3">
                              <Camera className="h-6 w-6" />
                            </span>
                          )}
                          <span className="text-sm font-medium">{photoName || "upload image"}</span>
                          <input
                            id="horse-photo"
                            type="file"
                            accept="image/*"
                            className="hidden"
                            onChange={handleFileChange}
                          />
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

                        <Button
                          onClick={() => {
                            setGenerationRound(0);
                            generateCaptions(0);
                          }}
                          className="w-full"
                        >
                          <Sparkles className="mr-2 h-4 w-4" />
                          generate 4 captions
                        </Button>
                      </CardContent>
                    </Card>
                  </>
                ) : null}

                {createStep === "captions" ? (
                  <Card className="caption-options-panel rounded-lg border-stone-200 shadow-sm">
                    <CardHeader className="pb-3">
                      <div className="caption-options-header">
                        <CardTitle className="text-base">caption options</CardTitle>
                        <Button onClick={refreshCaptions} variant="secondary" className="rounded-md">
                          <RefreshCcw className="mr-2 h-4 w-4" />
                          refresh 4 more
                        </Button>
                      </div>
                    </CardHeader>
                    <CardContent className="caption-card-content">
                      <div className="caption-carousel-bar">
                        <button
                          type="button"
                          className="caption-step-button"
                          aria-label="Previous caption option"
                          onClick={() => showCaptionAt(visibleCaptionIndex - 1)}
                          disabled={visibleCaptionIndex === 0}
                        >
                          <ArrowLeft className="h-4 w-4" />
                          Prev
                        </button>
                        <div className="caption-carousel-count">
                          {generated.length > 0 ? `${visibleCaptionIndex + 1}/${generated.length}` : "0/0"}
                        </div>
                        <button
                          type="button"
                          className="caption-step-button"
                          aria-label="Next caption option"
                          onClick={() => showCaptionAt(visibleCaptionIndex + 1)}
                          disabled={visibleCaptionIndex >= generated.length - 1}
                        >
                          Next
                          <ArrowRight className="h-4 w-4" />
                        </button>
                      </div>

                      <div
                        ref={captionScrollRef}
                        className="caption-scroll snap-x"
                        aria-label="Swipe caption options"
                        onScroll={handleCaptionScroll}
                      >
                        {generated.map((item, index) => {
                          const isSelected = selectedCaption === item.text;

                          return (
                            <button
                              key={item.id}
                              type="button"
                              data-caption-index={index}
                              onClick={() => setSelectedCaption(item.text)}
                              aria-pressed={isSelected}
                              className={`caption-option-card snap-center ${isSelected ? "is-active" : ""}`}
                            >
                              <span className="caption-preview-media">
                                {photoUrl ? (
                                  <img
                                    src={photoUrl}
                                    alt={photoName || "caption preview"}
                                    className="caption-preview-img"
                                  />
                                ) : (
                                  <span className="caption-preview-empty">
                                    <ImageIcon className="h-6 w-6" />
                                    <span>image preview</span>
                                  </span>
                                )}
                                <span className="caption-preview-shade" />
                                <span className="caption-preview-top">
                                  <Badge data-active={isSelected}>option {index + 1}</Badge>
                                  {isSelected ? (
                                    <span className="caption-selected-mark">
                                      <Check className="h-4 w-4" />
                                    </span>
                                  ) : null}
                                </span>
                                <span className="caption-preview-copy">{item.text}</span>
                              </span>
                            </button>
                          );
                        })}
                      </div>

                      <div className="caption-save-row">
                        <Button disabled={!selectedCaption} onClick={savePost}>
                          <Check className="mr-2 h-4 w-4" />
                          save selected caption
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                ) : null}
              </div>
            ) : null}

        {screen === "logs" ? (
          <div className="screen-stack">
            {savedPosts.length === 0 ? (
              <Card className="rounded-lg border-stone-200 shadow-sm">
                <CardContent className="py-10 text-center text-sm">No saved posts yet.</CardContent>
              </Card>
            ) : (
              savedPosts.map((post) => (
                <Card key={post.id} className="log-card rounded-lg border-stone-200 shadow-sm">
                  <CardContent>
                    <div className="log-card-meta">
                      <Badge className="rounded-full bg-stone-900 text-white">{post.postType}</Badge>
                      <div className="log-card-date">{new Date(post.createdAt).toLocaleDateString()}</div>
                    </div>
                    <div className="log-post-body">
                      <div className="log-thumb">
                        {post.photoUrl ? (
                          <img src={post.photoUrl} alt={post.photoName || "horse"} className="log-thumb-img" />
                        ) : (
                          <div className="log-thumb-empty">
                            <ImageIcon className="h-5 w-5" />
                          </div>
                        )}
                      </div>
                      <div className="log-copy-area">
                        <p className="log-caption">{post.caption}</p>
                        <Button
                          onClick={() => copyCaption(post.caption, post.id)}
                          variant="secondary"
                          className="log-copy-button rounded-md"
                        >
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
          <div className="screen-stack">
            <Card className="rounded-lg border-stone-200 shadow-sm">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">month totals vs expected</CardTitle>
              </CardHeader>
              <CardContent className="flex flex-col gap-4">
                {dashboardRows.map((row) => (
                  <div key={row.monthKey} className="tap-panel-content rounded-md border border-[var(--border-soft)]">
                    <div className="mb-3 flex items-center justify-between">
                      <div className="font-medium">{formatMonth(row.monthKey)}</div>
                      <div className="row-tag">
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
                              <span className="row-tag">
                                {done}/{expected}
                              </span>
                            </div>
                            <div className="progress-track">
                              <div className="progress-fill" style={{ width: `${pct}%` }} />
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
          </motion.div>
        </main>

        <nav className="app-nav">
          <div className="nav-strip" aria-label="Main app navigation">
            <button
              type="button"
              onClick={() => setScreen("start")}
              aria-pressed={screen === "start"}
              className={`nav-btn ${screen === "start" ? "is-active" : ""}`}
            >
              <span>Start</span>
            </button>
            <button
              type="button"
              onClick={() => setScreen("create")}
              aria-pressed={screen === "create"}
              className={`nav-btn ${screen === "create" ? "is-active" : ""}`}
            >
              <span>Create</span>
            </button>
            <button
              type="button"
              onClick={() => setScreen("logs")}
              aria-pressed={screen === "logs"}
              className={`nav-btn ${screen === "logs" ? "is-active" : ""}`}
            >
              <span>Logs</span>
            </button>
            <button
              type="button"
              onClick={() => setScreen("dashboard")}
              aria-pressed={screen === "dashboard"}
              className={`nav-btn ${screen === "dashboard" ? "is-active" : ""}`}
            >
              <span>Dashboard</span>
            </button>
          </div>
        </nav>
      </div>
    </div>
  );
}
