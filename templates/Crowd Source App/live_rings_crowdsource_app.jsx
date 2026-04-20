import React, { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { ChevronDown, ChevronRight, ChevronUp, CircleDot, Clock3, House, Radio, RefreshCw, ShieldCheck } from "lucide-react";
import { motion } from "framer-motion";

const COMMENT_LIBRARY = [
  "Running ahead of time",
  "Running behind",
  "In gate now",
  "Walk course soon",
  "Awards coming up",
  "Class delayed",
  "Course hold",
  "Jump-off starting",
  "Results posted",
  "Ring change announced",
  "Schooling active",
  "Crowded in warmup",
];

const SAMPLE_PAYLOAD = {
  show_id: "200000059",
  show_name: "2026 ESP Spring 3 (#5028) CSI 3*",
  json_data: [
    { class_group_id: "200023315", group_name: "$35,000 Arthramid National Grand Prix", estimated_start_time: "14:05:06", ring_number: "1", classes: ["200024170"], classNumbers: ["804"], status: "Underway", gone: "22", total: "39", is_live: true, curr_updated_at: "1776639839", ring_id: "10", ring: "Equestrian Village Derby Field", trips: "22/39" },
    { class_group_id: "200023314", group_name: "$120,000 CSI3* 1.50m Palm Beach County Sports Commission Grand Prix", estimated_start_time: "10:45:05", ring_number: "1", classes: ["200024008"], classNumbers: ["3014"], status: "Completed", gone: "43", total: "43", is_live: false, curr_updated_at: "1776632917", ring_id: "10", ring: "Equestrian Village Derby Field", trips: "43/43" },
    { class_group_id: "200023313", group_name: "$5,000 1.40m Junior/Amateur Jumper Classic", estimated_start_time: "08:00:07", ring_number: "1", classes: ["200024169"], classNumbers: ["708"], status: "Completed", gone: "29", total: "29", is_live: false, curr_updated_at: "1776619453", ring_id: "10", ring: "Equestrian Village Derby Field", trips: "29/29" },

    { class_group_id: "200023316", group_name: "$5,000 NAL 1.20m Junior & Amateur Jumper Classics", estimated_start_time: "09:00:00", ring_number: "3", classes: ["200024086", "200025724"], classNumbers: ["723", "724"], status: "Completed", gone: "47", total: "47", is_live: false, curr_updated_at: "1776627020", ring_id: "44", ring: "VanKampen Covered Arena", trips: "47/47" },
    { class_group_id: "200023317", group_name: "$5,000 NAL 1.30m Junior/Amateur Jumper Classic", estimated_start_time: "12:00:10", ring_number: "3", classes: ["200024047"], classNumbers: ["775"], status: "Completed", gone: "28", total: "28", is_live: false, curr_updated_at: "1776633560", ring_id: "44", ring: "VanKampen Covered Arena", trips: "28/28" },
    { class_group_id: "200023318", group_name: "$2,500 1.10m Schooling Jumper Classic", estimated_start_time: "14:10:00", ring_number: "3", classes: ["200024900", "200024901"], classNumbers: ["731", "732"], status: "Upcoming", gone: "0", total: "31", is_live: false, curr_updated_at: "1776635000", ring_id: "44", ring: "VanKampen Covered Arena", trips: "0/31" },

    { class_group_id: "200023320", group_name: "Amateur Owner Hunter 3'3\" 18-35", estimated_start_time: "08:00:00", ring_number: "5", classes: ["200024161", "200024286", "200024355"], classNumbers: ["379", "380", "910"], status: "Completed", gone: "25", total: "25", is_live: false, curr_updated_at: "1776628906", ring_id: "51", ring: "WI International Ring", trips: "25/25" },
    { class_group_id: "200023322", group_name: "Junior Hunter 3'6\" 15 & Under", estimated_start_time: "11:05:00", ring_number: "5", classes: ["200023989", "200023988", "200025723"], classNumbers: ["304", "305", "911"], status: "Completed", gone: "22", total: "22", is_live: false, curr_updated_at: "1776637154", ring_id: "51", ring: "WI International Ring", trips: "22/22" },
    { class_group_id: "200023328", group_name: "Junior Hunter 3'3\" 15 & Under", estimated_start_time: "14:55:06", ring_number: "5", classes: ["200024129", "200023891", "200024142"], classNumbers: ["339", "340", "909"], status: "Underway", gone: "3", total: "22", is_live: true, curr_updated_at: "1776639887", ring_id: "51", ring: "WI International Ring", trips: "3/22" },
    { class_group_id: "200023329", group_name: "Junior Hunter 3'3\" 16-17", estimated_start_time: "16:10:00", ring_number: "5", classes: ["200024239", "200024255", "200024142"], classNumbers: ["344", "345", "909"], status: "Upcoming", gone: "0", total: "24", is_live: false, curr_updated_at: "1776626003", ring_id: "51", ring: "WI International Ring", trips: "0/24" },

    { class_group_id: "200023331", group_name: "NCEA/USEF Junior Hunter Seat Medal", estimated_start_time: "09:00:09", ring_number: "6", classes: ["200024362"], classNumbers: ["585"], status: "Completed", gone: "8", total: "8", is_live: false, curr_updated_at: "1776619212", ring_id: "9", ring: "E.R. Mische Grand Hunter", trips: "8/8" },
    { class_group_id: "200023333", group_name: "ASPCA Maclay", estimated_start_time: "11:00:00", ring_number: "6", classes: ["200024066"], classNumbers: ["570"], status: "Completed", gone: "19", total: "19", is_live: false, curr_updated_at: "1776627960", ring_id: "9", ring: "E.R. Mische Grand Hunter", trips: "19/19" },
    { class_group_id: "200023365", group_name: "12-14 Equitation", estimated_start_time: "14:15:09", ring_number: "6", classes: ["200024266", "200024124", "200023998"], classNumbers: ["513", "514", "662"], status: "Underway", gone: "16", total: "17", is_live: true, curr_updated_at: "1776639661", ring_id: "9", ring: "E.R. Mische Grand Hunter", trips: "16/17" },
    { class_group_id: "200023366", group_name: "15-17 Equitation", estimated_start_time: "15:20:00", ring_number: "6", classes: ["200024267", "200024268"], classNumbers: ["515", "516"], status: "Upcoming", gone: "0", total: "18", is_live: false, curr_updated_at: "1776637000", ring_id: "9", ring: "E.R. Mische Grand Hunter", trips: "0/18" },
  ],
};

const MOCK_USERS = {
  guest: { id: "guest", name: "Guest", rating: "C" },
  trusted: { id: "trusted", name: "Trusted User", rating: "A" },
};

const CLASS_SHADE_STYLES = [
  "bg-amber-50 border-amber-200",
  "bg-blue-50 border-blue-200",
  "bg-emerald-50 border-emerald-200",
  "bg-rose-50 border-rose-200",
  "bg-violet-50 border-violet-200",
];

function toEpochSeconds(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function formatUpdated(epochSeconds) {
  if (!epochSeconds) return "Unknown";
  return new Date(epochSeconds * 1000).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

function normalizePayload(payload) {
  const ringsMap = new Map();

  for (const item of payload.json_data || []) {
    const ringKey = item.ring_id || item.ring_number || item.ring;

    if (!ringsMap.has(ringKey)) {
      ringsMap.set(ringKey, {
        ringId: ringKey,
        ringName: item.ring || `Ring ${item.ring_number}`,
        ringNumber: item.ring_number,
        updatedAt: toEpochSeconds(item.curr_updated_at),
        liveCount: 0,
        groups: [],
      });
    }

    const ring = ringsMap.get(ringKey);
    ring.updatedAt = Math.max(ring.updatedAt, toEpochSeconds(item.curr_updated_at));
    if (item.is_live || String(item.status).toLowerCase() === "underway") ring.liveCount += 1;

    ring.groups.push({
      groupId: item.class_group_id,
      groupName: item.group_name,
      status: item.status,
      isLive: Boolean(item.is_live) || String(item.status).toLowerCase() === "underway",
      estimatedStartTime: item.estimated_start_time,
      trips: item.trips,
      gone: item.gone,
      total: item.total,
      updatedAt: toEpochSeconds(item.curr_updated_at),
      ringId: ringKey,
      ringName: item.ring || `Ring ${item.ring_number}`,
      ringNumber: item.ring_number,
      classes: (item.classes || []).map((classId, idx) => ({
        classId,
        classNumber: item.classNumbers?.[idx] || "—",
        groupId: item.class_group_id,
        ringId: ringKey,
        ringName: item.ring || `Ring ${item.ring_number}`,
        status: item.status,
      })),
    });
  }

  return {
    showId: payload.show_id,
    showName: payload.show_name,
    rings: Array.from(ringsMap.values())
      .map((ring) => ({
        ...ring,
        groups: ring.groups.sort((a, b) => {
          if (a.isLive !== b.isLive) return a.isLive ? -1 : 1;
          return String(a.estimatedStartTime).localeCompare(String(b.estimatedStartTime));
        }),
      }))
      .sort((a, b) => {
        if (a.liveCount !== b.liveCount) return b.liveCount - a.liveCount;
        return String(a.ringNumber).localeCompare(String(b.ringNumber), undefined, { numeric: true });
      }),
    updatedAt: Math.max(0, ...Array.from(ringsMap.values()).map((r) => r.updatedAt)),
  };
}

function buildMockComments(scope, count, authorBase = "User", meta = {}) {
  return Array.from({ length: count }).map((_, idx) => ({
    id: `${scope}-${idx + 1}`,
    scope,
    text: COMMENT_LIBRARY[idx % COMMENT_LIBRARY.length],
    type: "library",
    author: `${authorBase} ${idx + 1}`,
    createdAt: Date.now() - idx * 1000 * 60 * 3,
    ...meta,
  }));
}

function seedComments(data) {
  const seeded = {};
  for (const ring of data.rings) {
    seeded[`ring:${ring.ringId}`] = buildMockComments(`ring:${ring.ringId}`, 4, "Ring User", { commentLevel: "ring", ringId: ring.ringId });
    for (const group of ring.groups) {
      seeded[`group:${group.groupId}`] = buildMockComments(`group:${group.groupId}`, 8, "Group User", { commentLevel: "group", groupId: group.groupId, ringId: ring.ringId });
      for (const cls of group.classes) {
        seeded[`class:${cls.classId}`] = buildMockComments(`class:${cls.classId}`, 3, `Class ${cls.classNumber}`, {
          commentLevel: "class",
          classId: cls.classId,
          classNumber: cls.classNumber,
          groupId: group.groupId,
          ringId: ring.ringId,
        });
      }
    }
  }
  return seeded;
}

function getMergedGroupComments(group, commentsByScope) {
  const groupComments = (commentsByScope[`group:${group.groupId}`] || []).map((comment) => ({ ...comment, commentLevel: "group" }));
  const classComments = group.classes.flatMap((cls) =>
    (commentsByScope[`class:${cls.classId}`] || []).map((comment) => ({
      ...comment,
      commentLevel: "class",
      classId: cls.classId,
      classNumber: cls.classNumber,
      groupId: group.groupId,
      ringId: group.ringId,
    }))
  );
  return [...groupComments, ...classComments].sort((a, b) => b.createdAt - a.createdAt);
}

function getMergedRingComments(ring, commentsByScope) {
  const ringComments = (commentsByScope[`ring:${ring.ringId}`] || []).map((comment) => ({ ...comment, commentLevel: "ring", ringId: ring.ringId }));
  const nested = ring.groups.flatMap((group) => getMergedGroupComments(group, commentsByScope));
  return [...ringComments, ...nested].sort((a, b) => b.createdAt - a.createdAt);
}

function getClassShade(comment) {
  const raw = String(comment.classId || comment.classNumber || "0");
  let hash = 0;
  for (let i = 0; i < raw.length; i += 1) hash = (hash * 31 + raw.charCodeAt(i)) % CLASS_SHADE_STYLES.length;
  return CLASS_SHADE_STYLES[hash];
}

function StatusBadge({ status, live }) {
  const normalized = String(status || "").toLowerCase();
  const cls = live
    ? "bg-red-100 text-red-700 border-red-200"
    : normalized === "completed"
      ? "bg-zinc-100 text-zinc-700 border-zinc-200"
      : normalized === "upcoming"
        ? "bg-blue-100 text-blue-700 border-blue-200"
        : "bg-amber-100 text-amber-700 border-amber-200";
  return <Badge className={`border ${cls}`}>{live ? "Live" : status || "Unknown"}</Badge>;
}

function TimelineCommentCard({ comment }) {
  const isClassComment = comment.commentLevel === "class";
  const rowClass = isClassComment ? getClassShade(comment) : comment.commentLevel === "ring" ? "bg-slate-50 border-slate-200" : "bg-zinc-50 border-zinc-200";
  const timestamp = new Date(comment.createdAt).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });

  return (
    <div className={`rounded-2xl border px-3 py-3 ${rowClass}`}>
      <div className="flex items-center gap-2 text-xs text-zinc-500 sm:hidden">
        <Badge variant="secondary" className="rounded-xl px-2">
          {comment.commentLevel === "class" ? `C${comment.classNumber}` : comment.commentLevel === "ring" ? "Ring" : "Group"}
        </Badge>
        <span>{timestamp}</span>
        <span className="ml-auto truncate">{comment.author}</span>
      </div>
      <div className="mt-2 text-sm font-medium leading-5 text-zinc-900 sm:hidden">{comment.text}</div>

      <div className="hidden sm:grid sm:grid-cols-[70px_70px_minmax(0,1fr)_70px] sm:items-center sm:gap-3">
        <div className="w-[70px] text-left">
          <Badge variant="secondary" className="w-full justify-center rounded-xl px-2">
            {comment.commentLevel === "class" ? `C${comment.classNumber}` : comment.commentLevel === "ring" ? "Ring" : "Group"}
          </Badge>
        </div>
        <div className="w-[70px] whitespace-nowrap text-left text-xs text-zinc-500">{timestamp}</div>
        <div className="min-w-0 truncate text-left text-sm font-medium text-zinc-900">{comment.text}</div>
        <div className="w-[70px] truncate text-right text-xs text-zinc-500">{comment.author}</div>
      </div>
    </div>
  );
}

function ScopeComments({ open, onOpenChange, title, scopeKey, onAddLibraryComment, onAddFreeComment, user, onAfterAdd }) {
  const [selectedLibrary, setSelectedLibrary] = useState(COMMENT_LIBRARY[0]);
  const [freeText, setFreeText] = useState("");
  const canFreeComment = user.rating === "A" || user.rating === "X";

  useEffect(() => {
    if (!open) {
      setSelectedLibrary(COMMENT_LIBRARY[0]);
      setFreeText("");
    }
  }, [open]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg rounded-3xl overflow-hidden p-0">
        <DialogHeader className="px-5 pt-5 pb-2">
          <DialogTitle className="text-lg font-semibold">{title}</DialogTitle>
        </DialogHeader>

        <div className="max-h-[80vh] space-y-5 overflow-y-auto px-5 pb-5">
          <div>
            <div className="mb-2 text-sm font-medium">Approved comment library</div>
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
              {COMMENT_LIBRARY.map((item) => (
                <button
                  key={item}
                  type="button"
                  onClick={() => setSelectedLibrary(item)}
                  className={`rounded-2xl border p-3 text-left text-sm transition ${selectedLibrary === item ? "border-zinc-900 bg-zinc-900 text-white" : "border-zinc-200 bg-white"}`}
                >
                  {item}
                </button>
              ))}
            </div>
            <Button
              className="mt-3 w-full rounded-2xl"
              onClick={() => {
                onAddLibraryComment(scopeKey, selectedLibrary, user.name);
                onAfterAdd?.();
              }}
            >
              Add selected comment
            </Button>
          </div>

          <div className="rounded-2xl border border-zinc-200 p-4">
            <div className="mb-2 flex items-center gap-2 text-sm font-medium">
              <ShieldCheck className="h-4 w-4" />
              Free comment access
            </div>
            <div className="mb-3 text-sm text-zinc-600">Users rated A or X can post custom comments.</div>
            <Textarea
              value={freeText}
              onChange={(e) => setFreeText(e.target.value)}
              placeholder={canFreeComment ? "Type a live update..." : "Upgrade user rating to A or X to unlock free comments"}
              disabled={!canFreeComment}
              className="min-h-24 rounded-2xl"
            />
            <Button
              className="mt-3 w-full rounded-2xl"
              disabled={!canFreeComment || !freeText.trim()}
              onClick={() => {
                onAddFreeComment(scopeKey, freeText.trim(), user.name);
                setFreeText("");
                onAfterAdd?.();
              }}
            >
              Post custom comment
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function StickyHeaderShell({ title, subtitle, filters, activeFilter, setActiveFilter, onComment, onBackHome }) {
  return (
    <div className="sticky top-0 z-30 bg-zinc-50/95 pb-2 pt-1 backdrop-blur">
      <Card className="overflow-hidden rounded-[22px] border-0 bg-gradient-to-br from-zinc-950 to-zinc-800 text-white shadow-sm">
        <CardContent className="p-3 sm:p-4">
          <div className="space-y-3">
            <div className="flex items-start gap-2 sm:gap-3">
              <div className="min-w-0 flex-1">
                <div className="text-xs uppercase tracking-[0.18em] text-zinc-300">{subtitle}</div>
                <h1 className="mt-1 text-lg font-semibold leading-tight sm:text-2xl">{title}</h1>
              </div>
              <Button variant="secondary" className="h-10 rounded-2xl px-3 shrink-0" onClick={onComment}>
                Comment
              </Button>
              <Button variant="secondary" size="icon" className="h-10 w-10 shrink-0 rounded-2xl" onClick={onBackHome}>
                <House className="h-4 w-4" />
              </Button>
            </div>

            <div className="overflow-x-auto pb-1">
              <div className="flex min-w-max gap-2">
                {filters.map((pill) => {
                  const isActive = activeFilter === pill.key;
                  return (
                    <button
                      key={pill.key}
                      type="button"
                      onClick={() => setActiveFilter(pill.key)}
                      className={`rounded-full border px-4 py-2 text-sm whitespace-nowrap transition ${isActive ? "border-white bg-white text-zinc-900" : "border-white/20 bg-white/10 text-white"}`}
                    >
                      {pill.label}
                    </button>
                  );
                })}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function GroupDetailPage({ group, commentsByScope, onBackHome, onComment }) {
  const [activeFilter, setActiveFilter] = useState("all");
  if (!group) return null;

  const filters = [
    { key: "all", label: "All" },
    { key: "group", label: "Group" },
    ...group.classes.map((cls) => ({ key: `class:${cls.classId}`, label: `C${cls.classNumber}` })),
  ];

  const comments = getMergedGroupComments(group, commentsByScope)
    .filter((comment) => {
      if (activeFilter === "all") return true;
      if (activeFilter === "group") return comment.commentLevel === "group";
      return `class:${comment.classId}` === activeFilter;
    })
    .slice(0, 40);

  return (
    <div className="min-h-screen bg-zinc-50 text-zinc-900">
      <div className="mx-auto w-full max-w-4xl px-0 py-3 sm:px-4 max-[360px]:max-w-[360px]">
        <StickyHeaderShell
          title={group.groupName}
          subtitle={`${group.ringName} • Ring ${group.ringNumber} • ${group.trips || `${group.gone}/${group.total}`}`}
          filters={filters}
          activeFilter={activeFilter}
          setActiveFilter={setActiveFilter}
          onComment={onComment}
          onBackHome={onBackHome}
        />
        <div className="space-y-2 pt-2">
          {comments.map((comment) => <TimelineCommentCard key={comment.id} comment={comment} />)}
        </div>
      </div>
    </div>
  );
}

function RingDetailPage({ ring, commentsByScope, onBackHome, onComment }) {
  const [activeFilter, setActiveFilter] = useState("all");
  if (!ring) return null;

  const filters = [
    { key: "all", label: "All" },
    { key: "ring", label: "Ring" },
    ...ring.groups.map((group) => ({ key: `group:${group.groupId}`, label: `G${group.groupId.slice(-3)}` })),
  ];

  const comments = getMergedRingComments(ring, commentsByScope)
    .filter((comment) => {
      if (activeFilter === "all") return true;
      if (activeFilter === "ring") return comment.commentLevel === "ring";
      return `group:${comment.groupId}` === activeFilter;
    })
    .slice(0, 60);

  return (
    <div className="min-h-screen bg-zinc-50 text-zinc-900">
      <div className="mx-auto w-full max-w-5xl px-0 py-3 sm:px-4 max-[360px]:max-w-[360px]">
        <StickyHeaderShell
          title={ring.ringName}
          subtitle={`Ring ${ring.ringNumber} • ${ring.groups.length} groups • ${ring.liveCount} live`}
          filters={filters}
          activeFilter={activeFilter}
          setActiveFilter={setActiveFilter}
          onComment={onComment}
          onBackHome={onBackHome}
        />
        <div className="space-y-2 pt-2">
          {comments.map((comment) => <TimelineCommentCard key={comment.id} comment={comment} />)}
        </div>
      </div>
    </div>
  );
}

function RingCard({ ring, commentsByScope, onOpenRing, onOpenGroup }) {
  const [open, setOpen] = useState(ring.liveCount > 0);
  return (
    <Card className="ring-card overflow-hidden rounded-[12px] border-0 shadow-sm" data-ring-id={ring.ringId}>
      <button type="button" onClick={() => setOpen((v) => !v)} className="w-full text-left">
        <CardHeader className="pb-3">
          <div className="ring-card-flex grid grid-cols-[75%_25%] items-start gap-3 border-b border-zinc-200 pb-3 max-[360px]:flex max-[360px]:flex-col">
            <div className="ring-card-title-wrapper flex min-w-0 flex-col">
              <CardTitle className="ring-card-title text-lg leading-tight sm:text-xl">{ring.ringName}</CardTitle>
              <div className="ring-card-subtitle mt-2 flex flex-wrap items-center gap-2 text-sm text-zinc-500">
                <span>Ring {ring.ringNumber}</span>
                <span>•</span>
                <span>{ring.groups.length} groups</span>
                <span>•</span>
                <span>{formatUpdated(ring.updatedAt)}</span>
              </div>
            </div>

            <div className="ring-card-action-wrapper flex flex-row items-center justify-end gap-1.5">
              <div>
                {ring.liveCount > 0 ? (
                  <Badge className="whitespace-nowrap rounded-xl border border-red-200 bg-red-100 px-2 py-1 text-red-700">{ring.liveCount} live</Badge>
                ) : (
                  <Badge className="whitespace-nowrap rounded-xl border border-zinc-200 bg-zinc-100 px-2 py-1 text-zinc-600">Idle</Badge>
                )}
              </div>
              <Button className="h-10 min-w-[104px] rounded-2xl px-3" variant="outline" onClick={(e) => { e.stopPropagation(); onOpenRing(ring.ringId); }}>
                Ring comments
              </Button>
            </div>
          </div>
        </CardHeader>
      </button>

      <CardContent className="pt-0">

        {open && (
          <div className="space-y-3">
            {ring.groups.map((group) => (
              <div key={group.groupId} className="group-card rounded-[20px] border-0 bg-zinc-50 p-0 pb-3" data-group-id={group.groupId}>
                <div className="group-card-body space-y-3">
                  <div className="group-card-flex grid grid-cols-[75%_25%] items-start gap-3">
                    <div className="group-card-title-wrapper min-w-0">
                      <div className="group-card-title font-medium leading-snug">{group.groupName}</div>
                      <div className="group-card-subtitle mt-2 flex flex-wrap items-center gap-3 text-sm text-zinc-500">
                        <span className="inline-flex items-center gap-1"><Clock3 className="h-4 w-4" /> {group.estimatedStartTime}</span>
                        <span>{group.trips || `${group.gone}/${group.total}`}</span>
                      </div>
                    </div>

                    <div className="group-card-action-wrapper flex flex-row items-center justify-end gap-2">
                      <div className="group-card-status-wrapper">
                        <StatusBadge status={group.status} live={group.isLive} />
                      </div>
                      <Button variant="outline" className="group-card-action min-w-[104px] rounded-2xl px-3 h-10" onClick={() => onOpenGroup(group.groupId)}>
                        Group comments
                      </Button>
                    </div>
                  </div>

                  <div className="group-card-comments-wrapper">
                    <div className="group-card-comments space-y-2">
                    {getMergedGroupComments(group, commentsByScope).slice(0, 4).map((comment) => (
                      <TimelineCommentCard key={comment.id} comment={comment} />
                    ))}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export default function LiveRingsCrowdsourceApp() {
  const [pollSeconds, setPollSeconds] = useState(15);
  const [feed, setFeed] = useState(() => normalizePayload(SAMPLE_PAYLOAD));
  const [commentsByScope, setCommentsByScope] = useState(() => seedComments(normalizePayload(SAMPLE_PAYLOAD)));
  const [scopeModal, setScopeModal] = useState({ open: false, key: "", title: "", afterAdd: null });
  const [userMode, setUserMode] = useState("guest");
  const [page, setPage] = useState({ type: "home", ringId: null, groupId: null });
  const [showModerationModel, setShowModerationModel] = useState(false);
  const [showFeedShape, setShowFeedShape] = useState(false);
  const [showFeedTotals, setShowFeedTotals] = useState(false);
  const user = MOCK_USERS[userMode];

  useEffect(() => {
    const id = setInterval(() => {
      setFeed((current) => {
        const next = JSON.parse(JSON.stringify(current));
        next.updatedAt = Math.floor(Date.now() / 1000);
        next.rings = next.rings.map((ring, ringIndex) => ({
          ...ring,
          updatedAt: Math.floor(Date.now() / 1000) - ringIndex,
          groups: ring.groups.map((group, groupIndex) => {
            const liveBump = group.isLive ? (groupIndex % 2 === 0 ? 1 : 0) : 0;
            const currentGone = Number(group.gone || 0);
            const total = Number(group.total || 0);
            const nextGone = Math.min(total, currentGone + liveBump);
            const nextStatus = nextGone >= total ? "Completed" : group.status;
            return {
              ...group,
              gone: String(nextGone),
              status: nextStatus,
              trips: `${nextGone}/${total}`,
              updatedAt: Math.floor(Date.now() / 1000),
              isLive: nextStatus.toLowerCase() === "underway" && nextGone < total,
              classes: group.classes.map((cls) => ({ ...cls, status: nextStatus })),
            };
          }),
        }));
        return next;
      });
    }, pollSeconds * 1000);
    return () => clearInterval(id);
  }, [pollSeconds]);

  const totals = useMemo(() => {
    const rings = feed.rings.length;
    const groups = feed.rings.reduce((sum, ring) => sum + ring.groups.length, 0);
    const classes = feed.rings.reduce((sum, ring) => sum + ring.groups.reduce((n, g) => n + g.classes.length, 0), 0);
    return { rings, groups, classes };
  }, [feed]);

  const allGroups = useMemo(() => feed.rings.flatMap((ring) => ring.groups), [feed]);
  const activeRing = useMemo(() => feed.rings.find((ring) => ring.ringId === page.ringId) || null, [feed, page.ringId]);
  const activeGroup = useMemo(() => allGroups.find((group) => group.groupId === page.groupId) || null, [allGroups, page.groupId]);

  const addComment = (scopeKey, text, author, type) => {
    const classMatch = scopeKey.match(/^class:(.+)$/);
    const groupMatch = scopeKey.match(/^group:(.+)$/);
    const ringMatch = scopeKey.match(/^ring:(.+)$/);
    let meta = {};

    if (classMatch) {
      const classId = classMatch[1];
      const found = allGroups.flatMap((group) => group.classes.map((cls) => ({ ...cls, groupId: group.groupId, ringId: group.ringId }))).find((cls) => cls.classId === classId);
      meta = { commentLevel: "class", classId, classNumber: found?.classNumber, groupId: found?.groupId, ringId: found?.ringId };
    } else if (groupMatch) {
      const groupId = groupMatch[1];
      const found = allGroups.find((group) => group.groupId === groupId);
      meta = { commentLevel: "group", groupId, ringId: found?.ringId };
    } else if (ringMatch) {
      meta = { commentLevel: "ring", ringId: ringMatch[1] };
    }

    setCommentsByScope((prev) => ({
      ...prev,
      [scopeKey]: [{ id: `${scopeKey}-${Date.now()}`, scope: scopeKey, text, type, author, createdAt: Date.now(), ...meta }, ...(prev[scopeKey] || [])],
    }));
  };

  const openComments = (key, title) => setScopeModal({ open: true, key, title, afterAdd: null });
  const openRing = (ringId) => setPage({ type: "ring", ringId, groupId: null });
  const openGroup = (groupId) => setPage({ type: "group", ringId: null, groupId });
  const backHome = () => setPage({ type: "home", ringId: null, groupId: null });

  if (page.type === "group" && activeGroup) {
    return (
      <>
        <GroupDetailPage group={activeGroup} commentsByScope={commentsByScope} onComment={() => openComments(`group:${activeGroup.groupId}`, activeGroup.groupName)} onBackHome={backHome} />
        <ScopeComments
          open={scopeModal.open}
          onOpenChange={(open) => setScopeModal((s) => ({ ...s, open }))}
          title={scopeModal.title}
          scopeKey={scopeModal.key}
          user={user}
          onAddLibraryComment={(scopeKey, text, author) => addComment(scopeKey, text, author, "library")}
          onAddFreeComment={(scopeKey, text, author) => addComment(scopeKey, text, author, "free")}
          onAfterAdd={scopeModal.afterAdd}
        />
      </>
    );
  }

  if (page.type === "ring" && activeRing) {
    return (
      <>
        <RingDetailPage ring={activeRing} commentsByScope={commentsByScope} onComment={() => openComments(`ring:${activeRing.ringId}`, activeRing.ringName)} onBackHome={backHome} />
        <ScopeComments
          open={scopeModal.open}
          onOpenChange={(open) => setScopeModal((s) => ({ ...s, open }))}
          title={scopeModal.title}
          scopeKey={scopeModal.key}
          user={user}
          onAddLibraryComment={(scopeKey, text, author) => addComment(scopeKey, text, author, "library")}
          onAddFreeComment={(scopeKey, text, author) => addComment(scopeKey, text, author, "free")}
          onAfterAdd={scopeModal.afterAdd}
        />
      </>
    );
  }

  return (
    <div className="min-h-screen bg-zinc-50 text-zinc-900">
      <div className="mx-auto w-full max-w-6xl px-0 py-3 sm:px-4 max-[360px]:max-w-[360px]">
        <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="space-y-4">
          <Card className="page-head overflow-hidden rounded-[12px] border-0 bg-gradient-to-br from-zinc-950 to-zinc-800 text-white shadow-sm">
            <CardContent className="p-3 sm:p-5">
              <div className="space-y-4">
                <div className="page-head-flex grid grid-cols-[75%_25%] items-start gap-3">
                  <div className="page-head-title-wrapper min-w-0">
                    <div className="page-head-eyebrow flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-zinc-300">
                      <Radio className="h-4 w-4" /> Live rings board
                    </div>
                    <h1 className="page-head-title mt-2 text-2xl font-semibold leading-tight sm:text-3xl">{feed.showName}</h1>
                    <div className="page-head-subtitle mt-2 text-sm text-zinc-300">Updated {formatUpdated(feed.updatedAt)}</div>
                  </div>
                  <div className="page-head-action-wrapper flex justify-end">
                    <Tabs value={userMode} onValueChange={setUserMode} className="w-auto">
                    <TabsList className="rounded-2xl border border-white/10 bg-white/10">
                      <TabsTrigger value="guest" className="rounded-xl">Guest C</TabsTrigger>
                      <TabsTrigger value="trusted" className="rounded-xl">Trusted A</TabsTrigger>
                    </TabsList>
                  </Tabs>
                  </div>
                </div>

                <div className="page-head-controls grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto] sm:items-center">
                  <div className="flex flex-wrap gap-2">
                    <Button variant="outline" className="rounded-2xl border-white/15 bg-white/10 text-white hover:bg-white/15" onClick={() => setShowFeedTotals((v) => !v)}>
                      Feed totals {showFeedTotals ? <ChevronUp className="ml-2 h-4 w-4" /> : <ChevronDown className="ml-2 h-4 w-4" />}
                    </Button>
                    <Button variant="outline" className="rounded-2xl border-white/15 bg-white/10 text-white hover:bg-white/15" onClick={() => setShowModerationModel((v) => !v)}>
                      Moderation model {showModerationModel ? <ChevronUp className="ml-2 h-4 w-4" /> : <ChevronDown className="ml-2 h-4 w-4" />}
                    </Button>
                    <Button variant="outline" className="rounded-2xl border-white/15 bg-white/10 text-white hover:bg-white/15" onClick={() => setShowFeedShape((v) => !v)}>
                      Feed shape in UI {showFeedShape ? <ChevronUp className="ml-2 h-4 w-4" /> : <ChevronDown className="ml-2 h-4 w-4" />}
                    </Button>
                  </div>
                  <div className="w-full sm:w-32">
                    <Input type="number" min={5} step={5} value={pollSeconds} onChange={(e) => setPollSeconds(Math.max(5, Number(e.target.value) || 5))} className="h-11 rounded-2xl bg-white text-zinc-900" placeholder="Poll" />
                  </div>
                </div>

                {showFeedTotals && (
                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                    {[["Rings", totals.rings], ["Groups", totals.groups], ["Classes", totals.classes]].map(([label, value]) => (
                      <div key={label} className="rounded-2xl bg-white/10 p-4 backdrop-blur">
                        <div className="text-xs uppercase tracking-wide text-zinc-300">{label}</div>
                        <div className="mt-1 text-2xl font-semibold">{value}</div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {showModerationModel && (
            <div className="grid gap-3 sm:grid-cols-3">
              <div className="rounded-[20px] border border-zinc-200 bg-white p-3 text-sm text-zinc-600 shadow-sm"><div className="mb-1 font-medium text-zinc-900">Default users</div><div>Can only post from the approved library.</div></div>
              <div className="rounded-[20px] border border-zinc-200 bg-white p-3 text-sm text-zinc-600 shadow-sm"><div className="mb-1 font-medium text-zinc-900">Rated A or X</div><div>Can post freeform comments in addition to the library.</div></div>
              <div className="rounded-[20px] border border-zinc-200 bg-white p-3 text-sm text-zinc-600 shadow-sm"><div className="mb-1 font-medium text-zinc-900">Signed in</div><div>{user.name} • Rating {user.rating}</div></div>
            </div>
          )}

          {showFeedShape && (
            <div className="grid gap-3 sm:grid-cols-3">
              <div className="flex items-start gap-3 rounded-3xl border border-zinc-200 bg-white p-4 text-sm text-zinc-600 shadow-sm"><CircleDot className="mt-0.5 h-4 w-4" /><div><span className="font-medium text-zinc-900">Ring</span> expands into a mobile stack of groups.</div></div>
              <div className="flex items-start gap-3 rounded-3xl border border-zinc-200 bg-white p-4 text-sm text-zinc-600 shadow-sm"><CircleDot className="mt-0.5 h-4 w-4" /><div><span className="font-medium text-zinc-900">Group</span> shows a short merged timeline preview.</div></div>
              <div className="flex items-start gap-3 rounded-3xl border border-zinc-200 bg-white p-4 text-sm text-zinc-600 shadow-sm"><CircleDot className="mt-0.5 h-4 w-4" /><div><span className="font-medium text-zinc-900">Detail pages</span> keep sticky controls and horizontally scrollable filters.</div></div>
            </div>
          )}

          <div className="space-y-4">
            {feed.rings.map((ring) => (
              <RingCard key={ring.ringId} ring={ring} commentsByScope={commentsByScope} onOpenRing={openRing} onOpenGroup={openGroup} />
            ))}
          </div>
        </motion.div>
      </div>

      <ScopeComments
        open={scopeModal.open}
        onOpenChange={(open) => setScopeModal((s) => ({ ...s, open }))}
        title={scopeModal.title}
        scopeKey={scopeModal.key}
        user={user}
        onAddLibraryComment={(scopeKey, text, author) => addComment(scopeKey, text, author, "library")}
        onAddFreeComment={(scopeKey, text, author) => addComment(scopeKey, text, author, "free")}
        onAfterAdd={scopeModal.afterAdd}
      />
    </div>
  );
}
