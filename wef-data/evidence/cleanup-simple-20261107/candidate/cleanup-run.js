'use strict';
// Existing triggerless task; Records alerts enter the independent replacement lane.
require('./cleanup-lane').main().catch(error=>{console.error(error.message);process.exitCode=1;});
