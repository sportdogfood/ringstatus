declare module "rita" {
  export const RiTa: {
    tokenize(input: string): string[];
    grammar(rules: Record<string, string>): {
      expand(): string;
    };
  };

  const defaultExport: {
    RiTa: typeof RiTa;
  };

  export default defaultExport;
}
