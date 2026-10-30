import { Predicate } from "effect";

/** Object property order is not a configuration change; rule/list order remains significant. */
export function configurationState(value: unknown): string {
  const visiting = new WeakSet<object>();
  const canonical = (current: unknown): unknown => {
    if (current instanceof Date) return current.toISOString();
    if (Array.isArray(current) || Predicate.isObject(current)) {
      if (visiting.has(current)) throw new Error("Circular configuration");
      visiting.add(current);
      const result = Array.isArray(current)
        ? current.map(canonical)
        : Object.fromEntries(
            Object.keys(current)
              .toSorted()
              .filter((key) => current[key] !== undefined)
              .map((key) => [key, canonical(current[key])]),
          );
      visiting.delete(current);
      return result;
    }
    return current;
  };
  try {
    return JSON.stringify(canonical(value)) ?? "undefined";
  } catch {
    return "InvalidConfigurationState";
  }
}
