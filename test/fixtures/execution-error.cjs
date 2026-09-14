module.exports = (value) => {
  if (value === 2) throw new Error("fixture failure");
  return value;
};
