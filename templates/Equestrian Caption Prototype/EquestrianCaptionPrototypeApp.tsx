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

const starterPools: Record<PostType, string[]> = {
  "what i see": [
    "he finally stayed organized to the base today.",
    "the best part was how much softer he felt by the end.",
    "what mattered today was the feel, not the result.",
    "she felt much more rideable everywhere today.",
    "small thing, but he finally waited for me.",
    "the canter stayed together the whole trip and that mattered.",
    "he felt quieter through his body today.",
    "this was one of those rides where the details mattered most.",
  ],
  "what the horse sees": [
    "i was not simple today but she still stayed quiet.",
    "i gave her a lot to manage and she answered honestly.",
    "not every ride is fancy. some are just honest work.",
    "she asked me to do my job today.",
    "i made her think a lot and she still tried.",
    "i had ideas. she had better ones.",
    "she stayed patient even when i was figuring it out.",
    "today was less glamorous and more honest.",
  ],
  "what we did": [
    "focused on rhythm first and got a much better answer by the end.",
    "kept him soft and organized and he finished much more confident.",
    "worked on keeping the canter together and it paid off.",
    "quiet ride, better answer.",
    "kept the ride simple and he got better every minute.",
    "we stayed patient and it started to click.",
    "made a few small changes and got a much better feel.",
    "nothing dramatic, just better by the end.",
  ],
  "what we almost did": [
    "not a ribbon round, but a better horse by the end.",
    "did not get the prize, got the answer.",
    "not polished, not easy, but productive.",
    "still the kind of ride that matters.",
    "not a winning trip, still worth posting.",
    "not perfect, but a lot better.",
    "almost got the result, definitely got the progress.",
    "some rides do not win. they teach.",
  ],
  reality: [
    "early feed, tired legs, and one good ride before dark.",
    "barn dust, cold hands, and a long day.",
    "five horses later and still learning.",
    "nothing glamorous, just a lot of work.",
    "the kind of day that starts early and ends with one honest ride.",
    "worked all day, still worth it.",
    "real barn life and a horse i still wanted a picture of.",
    "just one of those long barn days.",
  ],
  confidence: [
    "not fancy. effective.",
    "soft, organized, better.",
    "earned.",
    "rideable.",
    "better.",
    "no spotlight needed.",
    "quietly better.",
    "useful.",
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

function buildCaption(base: string, description: string): string {
  const desc = description.trim();
  return desc ? `${base}\n${desc}` : base;
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
      text: buildCaption(base, description),
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

  return (
    <div className="min-h-screen bg-stone-100 text-stone-900">
      <div className="mx-auto max-w-md px-4 py-4">
        <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="mb-4">
          <div className="mb-2 text-xs font-semibold uppercase tracking-[0.22em] text-stone-500">
            mobile caption generator
          </div>
          <h1 className="text-2xl font-semibold tracking-tight">horse post builder</h1>
        </motion.div>

        <div className="mb-4 grid grid-cols-3 gap-2 rounded-lg border border-stone-200 bg-white p-2 shadow-sm">
          <button
            type="button"
            onClick={() => setScreen("create")}
            aria-pressed={screen === "create"}
            className={`rounded-md px-3 py-3 text-sm font-medium ${
              screen === "create" ? "bg-stone-900 text-white" : "bg-stone-100 text-stone-700"
            }`}
          >
            create
          </button>
          <button
            type="button"
            onClick={() => setScreen("log")}
            aria-pressed={screen === "log"}
            className={`rounded-md px-3 py-3 text-sm font-medium ${
              screen === "log" ? "bg-stone-900 text-white" : "bg-stone-100 text-stone-700"
            }`}
          >
            <span className="inline-flex items-center gap-2">
              <List className="h-4 w-4" />
              log
            </span>
          </button>
          <button
            type="button"
            onClick={() => setScreen("dashboard")}
            aria-pressed={screen === "dashboard"}
            className={`rounded-md px-3 py-3 text-sm font-medium ${
              screen === "dashboard" ? "bg-stone-900 text-white" : "bg-stone-100 text-stone-700"
            }`}
          >
            <span className="inline-flex items-center gap-2">
              <LayoutDashboard className="h-4 w-4" />
              dashboard
            </span>
          </button>
        </div>

        {screen === "create" ? (
          <div className="grid gap-4">
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
