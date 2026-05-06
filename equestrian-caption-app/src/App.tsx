"use client";

import React, { useMemo, useRef, useState } from "react";
import {
  ArrowLeft,
  ArrowRight,
  Camera,
  Check,
  Copy,
  Home,
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

type CaptionTag = {
  id: string;
  label: string;
  line: string;
  purpose: CaptionTagPurpose;
};

type CaptionTagGroups = Record<CaptionTagPurpose, CaptionTag[]>;

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

const tagOptionsByPostType: Record<PostType, CaptionTagGroups> = {
  "what i see": {
    detail: [
      { id: "wis-waiting-base", label: "waiting to base", line: "waited to the base.", purpose: "detail" },
      { id: "wis-organized-canter", label: "organized canter", line: "organized canter.", purpose: "detail" },
      { id: "wis-softer-change", label: "softer change", line: "softer change.", purpose: "detail" },
      { id: "wis-straightness", label: "straightness", line: "stayed straighter.", purpose: "detail" },
    ],
    tone: [
      { id: "wis-quietly-pleased", label: "quietly pleased", line: "small win.", purpose: "tone" },
      { id: "wis-dry", label: "dry", line: "finally.", purpose: "tone" },
      { id: "wis-matter-of-fact", label: "matter-of-fact", line: "that was the point.", purpose: "tone" },
      { id: "wis-soft", label: "soft", line: "i'll take it.", purpose: "tone" },
    ],
  },
  "what the horse sees": {
    detail: [
      { id: "whs-opinion", label: "opinion", line: "had an opinion.", purpose: "detail" },
      { id: "whs-weird-timing", label: "weird timing", line: "timing was a choice.", purpose: "detail" },
      { id: "whs-tolerated-me", label: "tolerated me", line: "tolerated me anyway.", purpose: "detail" },
      { id: "whs-chaos", label: "chaos", line: "provided character development.", purpose: "detail" },
    ],
    tone: [
      { id: "whs-deadpan", label: "deadpan", line: "fair enough.", purpose: "tone" },
      { id: "whs-sarcastic", label: "mildly sarcastic", line: "apparently.", purpose: "tone" },
      { id: "whs-playful", label: "playful", line: "i had notes.", purpose: "tone" },
      { id: "whs-low-affection", label: "low affection", line: "still tried.", purpose: "tone" },
    ],
  },
  "what we did": {
    detail: [
      { id: "wwd-rhythm-first", label: "rhythm first", line: "rhythm first.", purpose: "detail" },
      { id: "wwd-softer", label: "softer", line: "softer by the end.", purpose: "detail" },
      { id: "wwd-rideable", label: "rideable", line: "more rideable.", purpose: "detail" },
      { id: "wwd-brought-along", label: "brought along", line: "a little more together.", purpose: "detail" },
    ],
    tone: [
      { id: "wwd-practical", label: "practical", line: "useful day.", purpose: "tone" },
      { id: "wwd-quiet-pride", label: "quiet pride", line: "earned that.", purpose: "tone" },
      { id: "wwd-grounded", label: "grounded", line: "nothing dramatic.", purpose: "tone" },
      { id: "wwd-warm", label: "warm", line: "good little step.", purpose: "tone" },
    ],
  },
  "what we almost did": {
    detail: [
      { id: "wwad-one-rail", label: "one rail", line: "one rail.", purpose: "detail" },
      { id: "wwad-no-ribbon", label: "no ribbon", line: "no ribbon.", purpose: "detail" },
      { id: "wwad-better-answer", label: "better answer", line: "better answer after.", purpose: "detail" },
      { id: "wwad-useful-miss", label: "useful miss", line: "useful miss.", purpose: "detail" },
    ],
    tone: [
      { id: "wwad-calm", label: "calm", line: "still worth posting.", purpose: "tone" },
      { id: "wwad-tough", label: "tough", line: "kept riding.", purpose: "tone" },
      { id: "wwad-dry", label: "dry", line: "annoying. helpful.", purpose: "tone" },
      { id: "wwad-honest", label: "honest", line: "not wasted.", purpose: "tone" },
    ],
  },
  reality: {
    detail: [
      { id: "reality-long-day", label: "long barn day", line: "long barn day.", purpose: "detail" },
      { id: "reality-chores", label: "chores first", line: "chores first.", purpose: "detail" },
      { id: "reality-tired-legs", label: "tired legs", line: "tired legs.", purpose: "detail" },
      { id: "reality-weather", label: "weather", line: "barn weather.", purpose: "detail" },
    ],
    tone: [
      { id: "reality-dry", label: "dry", line: "normal.", purpose: "tone" },
      { id: "reality-funny", label: "funny", line: "fair trade.", purpose: "tone" },
      { id: "reality-matter-of-fact", label: "matter-of-fact", line: "still rode.", purpose: "tone" },
      { id: "reality-soft", label: "soft", line: "worth it.", purpose: "tone" },
    ],
  },
  confidence: {
    detail: [
      { id: "confidence-simple-win", label: "simple win", line: "simple win.", purpose: "detail" },
      { id: "confidence-strong-image", label: "strong image", line: "image says enough.", purpose: "detail" },
      { id: "confidence-rideable", label: "rideable", line: "rideable.", purpose: "detail" },
      { id: "confidence-earned-feel", label: "earned feel", line: "earned feel.", purpose: "detail" },
    ],
    tone: [
      { id: "confidence-final", label: "final", line: "enough said.", purpose: "tone" },
      { id: "confidence-understated", label: "understated", line: "quietly solid.", purpose: "tone" },
      { id: "confidence-sharper", label: "sharper", line: "useful.", purpose: "tone" },
      { id: "confidence-warm", label: "warm", line: "good one.", purpose: "tone" },
    ],
  },
};

const monthLabel = new Intl.DateTimeFormat("en-US", {
  month: "short",
  year: "numeric",
});

const EMPTY_TAG_IDS: string[] = [];

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

function buildTagSteeringLine(selectedTags: CaptionTag[]): string {
  const detailLines = selectedTags
    .filter((tag) => tag.purpose === "detail")
    .slice(0, 2)
    .map((tag) => tag.line);
  const toneLines = selectedTags
    .filter((tag) => tag.purpose === "tone")
    .slice(0, 1)
    .map((tag) => tag.line);

  return normalizeCaptionLine([...detailLines, ...toneLines].join(" "));
}

function buildCaption(base: string, description: string, type: PostType, selectedTags: CaptionTag[] = []): string {
  const rule = postTypeRules[type];
  const baseLine = normalizeCaptionLine(base);
  const desc = normalizeCaptionLine(description);
  const tagLine = buildTagSteeringLine(selectedTags);

  if (!desc && !tagLine) return baseLine;

  const maxSupportingLength = type === "confidence" ? 82 : 138;
  const descLength = tagLine ? (type === "confidence" ? 42 : 82) : maxSupportingLength;
  const descLine = desc ? shortenLine(desc, descLength) : "";
  const supportingLine = shortenLine([descLine, tagLine].filter(Boolean).join(" "), maxSupportingLength);

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

function VoiceProfileCard({ postType, activeRule }: { postType: PostType; activeRule: PostTypeRule }) {
  const knobEntries = Object.entries(voiceProfile.knobs).filter(([key]) => {
    return ["confidence", "humor", "softness", "maturity", "polish", "detail", "length"].includes(key);
  });
  const avoidList = [...activeRule.avoid.slice(0, 4), ...voiceProfile.avoid.slice(0, 3)];
  const availableLines = [
    ...voiceProfile.recurringSeries,
    ...highlightedLinesByPostType[postType].slice(0, 3),
  ];

  return (
    <Card className="rounded-lg border-stone-200 shadow-sm">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">voice rules in use</CardTitle>
      </CardHeader>
      <CardContent className="voice-rule-grid">
        <div className="voice-rule-block">
          <div className="voice-rule-title">why this shows</div>
          <p className="voice-rule-copy">
            These are the guardrails steering every caption option: teen voice, horse-specific detail, short length,
            and no adult-polished caption language.
          </p>
        </div>

        <div className="voice-rule-block">
          <div className="voice-rule-title">tone</div>
          <div className="voice-chip-wrap">
            {voiceProfile.coreTone.slice(0, 7).map((tone) => (
              <Badge key={tone} variant="secondary">
                {tone}
              </Badge>
            ))}
          </div>
        </div>

        <div className="voice-rule-block">
          <div className="voice-rule-title">{postType}</div>
          <div className="voice-rule-meta">target: {activeRule.maxLines} short lines max</div>
          <div className="voice-rule-list">
            {activeRule.leadWith.map((item) => (
              <div key={item} className="voice-rule-line">
                lead with {item}
              </div>
            ))}
          </div>
        </div>

        <div className="voice-rule-block">
          <div className="voice-rule-title">keep out</div>
          <div className="voice-chip-wrap">
            {avoidList.map((item) => (
              <Badge key={item} variant="outline">
                {item}
              </Badge>
            ))}
          </div>
        </div>

        <div className="voice-rule-block">
          <div className="voice-rule-title">settings</div>
          <div className="voice-chip-wrap">
            {knobEntries.map(([key, value]) => (
              <Badge key={key}>
                {key}: {value}
              </Badge>
            ))}
          </div>
          <div className="voice-rule-copy">
            {voiceProfile.generationChecklist.should.slice(0, 3).join(" / ")}
          </div>
        </div>

        <div className="voice-rule-block">
          <div className="voice-rule-title">must stay available</div>
          <div className="voice-chip-wrap">
            {availableLines.map((line) => (
              <Badge key={line} variant="outline">
                {line}
              </Badge>
            ))}
          </div>
        </div>
      </CardContent>
    </Card>
  );
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
            <div className="tag-pill-wrap">
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
  const [screen, setScreen] = useState<Screen>("start");
  const [createStep, setCreateStep] = useState<CreateStep>("postType");
  const [postType, setPostType] = useState<PostType>("what i see");
  const [description, setDescription] = useState("");
  const [photoUrl, setPhotoUrl] = useState("");
  const [photoName, setPhotoName] = useState("");
  const [generationRound, setGenerationRound] = useState(0);
  const [selectedCaption, setSelectedCaption] = useState("");
  const [savedPosts, setSavedPosts] = useState<SavedPost[]>([]);
  const [copiedId, setCopiedId] = useState("");
  const [generated, setGenerated] = useState<GeneratedCaption[]>([]);
  const [showVoiceProfile, setShowVoiceProfile] = useState(false);
  const [selectedTagIdsByType, setSelectedTagIdsByType] = useState<Partial<Record<PostType, string[]>>>({});
  const [visibleCaptionIndex, setVisibleCaptionIndex] = useState(0);
  const captionScrollRef = useRef<HTMLDivElement>(null);

  const monthKey = currentMonthKey();
  const activeRule = postTypeRules[postType];
  const createStepIndex = createSteps.findIndex((step) => step.value === createStep);
  const selectedTagIds = selectedTagIdsByType[postType] ?? EMPTY_TAG_IDS;
  const selectedTags = useMemo(() => {
    return getSelectedTagsForPostType(postType, selectedTagIds);
  }, [postType, selectedTagIds]);

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
    const pool = shiftPool(
      starterPools[postType].filter((line) => line !== highlightedLine),
      nextRound * 3,
    );
    const regularCaptions = pool.slice(0, 3).map((base, index) => ({
      id: `${postType}-${nextRound}-${index}`,
      text: buildCaption(base, description, postType, selectedTags),
    }));
    const highlightedCaption = {
      id: `${postType}-${nextRound}-highlight`,
      text: buildCaption(highlightedLine, "", postType, selectedTags),
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
    setCreateStep("tags");
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
  const headerActionLabel = screen === "start" ? "Start" : screen === "create" && createStep === "image" ? "Gen" : "Next";
  const showHeaderAction = screen === "start" || (screen === "create" && createStep !== "captions");
  const showHeaderBack = screen === "create";

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
                  <div className="start-logo-title">mobile caption generator</div>
                  <div className="start-logo-subtitle">horse post builder</div>
                </div>

                <Card className="rounded-lg border-stone-200 shadow-sm">
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">start</CardTitle>
                  </CardHeader>
                  <CardContent className="start-screen-content">
                    <label className="voice-toggle" htmlFor="voice-profile-toggle">
                      <input
                        id="voice-profile-toggle"
                        type="checkbox"
                        checked={showVoiceProfile}
                        onChange={(event) => setShowVoiceProfile(event.target.checked)}
                        aria-controls="voice-profile-panel"
                        className="voice-toggle-input"
                      />
                      <span className="voice-toggle-box" aria-hidden="true">
                        <Check className="h-3 w-3" />
                      </span>
                      <span>Voice Profile</span>
                    </label>

                    <Button onClick={startCreateFlow} className="w-full">
                      Start
                      <ArrowRight className="ml-2 h-4 w-4" />
                    </Button>
                  </CardContent>
                </Card>

                <div id="voice-profile-panel" className="voice-profile-panel" data-visible={showVoiceProfile}>
                  <VoiceProfileCard postType={postType} activeRule={activeRule} />
                </div>
              </div>
            ) : null}

            {screen === "create" ? (
              <div className="screen-stack create-screen-stack">
                {createStep !== "captions" ? <CreateProgress step={createStep} /> : null}

                {createStep === "postType" ? (
                  <Card className="rounded-lg border-stone-200 shadow-sm">
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
                          <div className="photo-drop flex h-24 items-center justify-center">
                            <ImageIcon className="h-5 w-5" />
                          </div>
                        )}
                      </div>
                      <div>
                        <p className="mb-2 whitespace-pre-wrap text-sm leading-6">{post.caption}</p>
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
          <div className="nav-strip">
            <button
              type="button"
              onClick={() => setScreen("start")}
              aria-pressed={screen === "start"}
              className={`nav-btn ${screen === "start" ? "is-active" : ""}`}
            >
              <Home className="h-4 w-4" />
              Start
            </button>
            <button
              type="button"
              onClick={() => setScreen("create")}
              aria-pressed={screen === "create"}
              className={`nav-btn ${screen === "create" ? "is-active" : ""}`}
            >
              <Sparkles className="h-4 w-4" />
              Create
            </button>
            <button
              type="button"
              onClick={() => setScreen("logs")}
              aria-pressed={screen === "logs"}
              className={`nav-btn ${screen === "logs" ? "is-active" : ""}`}
            >
              <List className="h-4 w-4" />
              Logs
            </button>
            <button
              type="button"
              onClick={() => setScreen("dashboard")}
              aria-pressed={screen === "dashboard"}
              className={`nav-btn ${screen === "dashboard" ? "is-active" : ""}`}
            >
              <LayoutDashboard className="h-4 w-4" />
              Dashboard
            </button>
          </div>
        </nav>
      </div>
    </div>
  );
}
