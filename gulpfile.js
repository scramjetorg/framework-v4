/* eslint-disable node/no-unpublished-require */
const gulp = require("gulp");
const path = require("path");
const shell = require("gulp-shell");

const {lint, test_legacy, readme, scm_clean} = require("scramjet-core/scripts/tasks");
const {full_docs, tsd} = require("./scripts/tasks");

const corepath = path.dirname(require.resolve("scramjet-core"));
const FILES = [
    path.dirname(require.resolve("scramjet-core")) + "/index.js",
    path.dirname(require.resolve("scramjet-core")) + "/data-stream.js",
    path.dirname(require.resolve("scramjet-core")) + "/string-stream.js",
    path.dirname(require.resolve("scramjet-core")) + "/buffer-stream.js",
    path.dirname(require.resolve("scramjet-core")) + "/multi-stream.js",
    "lib/index.js",
    "lib/data-stream.js",
    "lib/string-stream.js",
    "lib/buffer-stream.js",
    "lib/number-stream.js",
    "lib/window-stream.js",
    "lib/stream-worker.js",
    "lib/multi-stream.js"
];

process.env.SCRAMJET_TEST_HOME = __dirname;

gulp.task("lint", lint());

gulp.task("test_legacy", test_legacy([path.resolve(corepath, "../test/v1/*.js"), "test/v1/*.js"]));

gulp.task("scm_clean", scm_clean());

gulp.task("test_samples", shell.task("node scripts/test/test-samples"));

// const dtsTask = shell.task("npx check-dts", {cwd: "./.d.ts"});
// gulp.task("test_dts", async () => {
//     try {
//         await dtsTask();
//     } catch(e) {
//         if (process.version.split(".")[0] !== "v15") {
//             throw e;
//         }
//     }
// });

gulp.task("test_tsd", async () => {
    try {
        await shell.task("npm test", {cwd: "./test/ts-test"})();
    } catch(e) {
        if (process.version.split(".")[0] !== "v15") {
            throw e;
        }
    }
});


gulp.task("readme", readme({
    files: FILES.slice(),
    plugin: ["scramjet-core/jsdoc2md/plugin.js", "jsdoc2md/plugin.js",]
}, path.join(__dirname, "README.md")));

gulp.task("tsd", tsd(FILES.slice(), {
    plugins: ["jsdoc2md/plugin-tsd.js"],
    opts: {
        "tags": {
            "allowUnknownTags": true,
            "dictionaries": ["jsdoc","closure"]
        },
        prepend: "./jsdoc2md/tsd-template/prepend.d.ts",
        dataStreamInject: "./jsdoc2md/tsd-template/inject-data-stream.d.ts",
        template: "./jsdoc2md/tsd-template",
        destination: ".d.ts/index.d.ts"
    }
}));

gulp.task("copy_docs", function() {
    return gulp
        .src(path.resolve(corepath, "../docs/*"))
        .pipe(gulp.dest("docs/"));
});

gulp.task("make_docs", full_docs(
    ["lib/*.js"],
    corepath,
    {plugin: ["scramjet-core/jsdoc2md/plugin-docs.js"]},
    {plugin: ["scramjet-core/jsdoc2md/plugin-docs.js"]},
    "docs/"
));

gulp.task("docs", gulp.series("tsd", "readme", "copy_docs", "make_docs"));
gulp.task("test", gulp.series("test_legacy", "test_samples", "test_tsd"));
gulp.task("fulltest", gulp.series("lint", "test"));
gulp.task("default", gulp.series("readme", "docs", "test", "lint"));
gulp.task("prerelease", gulp.series("default", "scm_clean"));
