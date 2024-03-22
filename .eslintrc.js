module.exports = {
  root: true,
  plugins: [
    'no-only-tests'
  ],
  extends: ['standard'],
  // add your custom rules here
  rules: {
    // allow paren-less arrow functions
    'arrow-parens': 0
  }
}
