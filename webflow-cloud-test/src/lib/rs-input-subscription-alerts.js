// Exact native prototype keys, not approved engine trigger identifiers.
export const subscriptionAlertDefinitions = [
  {
    "key": "class_start_late",
    "group": "Classes",
    "label": "Class delayed",
    "description": "When a class is expected to start later than scheduled."
  },
  {
    "key": "class_start_early",
    "group": "Classes",
    "label": "Class moved earlier",
    "description": "When a class is expected to start earlier than scheduled."
  },
  {
    "key": "class_starts_in1",
    "group": "Classes",
    "label": "Class reminder 1",
    "description": "A reminder before the expected class start.",
    "input": "minutes",
    "presets": [
      30,
      45,
      60,
      75,
      90
    ]
  },
  {
    "key": "class_starts_in2",
    "group": "Classes",
    "label": "Class reminder 2",
    "description": "A second reminder before the expected class start.",
    "input": "minutes",
    "presets": [
      15,
      30,
      45,
      60
    ]
  },
  {
    "key": "class_started",
    "group": "Classes",
    "label": "Class started",
    "description": "When the class begins."
  },
  {
    "key": "class_underway",
    "group": "Classes",
    "label": "Class in progress",
    "description": "An update when the class is underway."
  },
  {
    "key": "class_completed",
    "group": "Classes",
    "label": "Class finished",
    "description": "When the class is complete."
  },
  {
    "key": "trip_starts_in1",
    "group": "Rider trips",
    "label": "Ride reminder 1",
    "description": "A reminder before the expected start of a rider’s trip.",
    "input": "minutes",
    "presets": [
      30,
      45,
      60,
      75,
      90
    ]
  },
  {
    "key": "trip_starts_in2",
    "group": "Rider trips",
    "label": "Ride reminder 2",
    "description": "A second reminder before the expected start of a rider’s trip.",
    "input": "minutes",
    "presets": [
      15,
      30,
      45,
      60
    ]
  },
  {
    "key": "rider_oog",
    "group": "Rider trips",
    "label": "Rider order",
    "description": "An update about the rider’s order of go."
  },
  {
    "key": "rider_oog10",
    "group": "Rider trips",
    "label": "Rides to go",
    "description": "Tell me when this many entries remain before my rider’s turn.",
    "input": "entries",
    "presets": [
      7,
      10,
      15
    ],
    "defaultValue": "10"
  },
  {
    "key": "rider_started",
    "group": "Rider trips",
    "label": "Rider started",
    "description": "When the rider begins their trip."
  },
  {
    "key": "rider_first_score",
    "group": "Rider trips",
    "label": "First score",
    "description": "When the rider’s first score is reported."
  },
  {
    "key": "rider_first_time",
    "group": "Rider trips",
    "label": "First time",
    "description": "When the rider’s first time is reported."
  },
  {
    "key": "rider_results",
    "group": "Rider trips",
    "label": "Rider results",
    "description": "When results for the rider become available."
  },
  {
    "key": "groom_tasks_at1",
    "group": "Groom tasks",
    "label": "Task reminder 1",
    "description": "A groom task reminder at your chosen time.",
    "input": "time"
  },
  {
    "key": "groom_tasks_at2",
    "group": "Groom tasks",
    "label": "Task reminder 2",
    "description": "A second groom task reminder at your chosen time.",
    "input": "time"
  },
  {
    "key": "groom_tasks_at3",
    "group": "Groom tasks",
    "label": "Task reminder 3",
    "description": "A third groom task reminder at your chosen time.",
    "input": "time"
  }
];
