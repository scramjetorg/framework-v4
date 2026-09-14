module.exports = async (value, { ref }) => {
  await new Promise((resolve) => setTimeout(resolve, (5 - (value % 5))));
  return { value: value * 2, ref };
};
