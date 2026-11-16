// Intentional defect for native Codex Code Review proof only.
// Business rule: calculateLeadMinutes(tripsRemaining, minutesPerTrip)
// must return tripsRemaining * minutesPerTrip.
export function calculateLeadMinutes(tripsRemaining, minutesPerTrip) {
  return tripsRemaining * 2;
}
