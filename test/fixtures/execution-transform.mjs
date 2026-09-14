export default async (value, { ref }) => {
  await new Promise((resolve) => setTimeout(resolve, 1));
  return { value: value + 1, ref };
};
