export default async (value, { ref }) => {
  await new Promise((resolve) => setTimeout(resolve, value === 1 ? 20 : 1));
  return { value, ref };
};
