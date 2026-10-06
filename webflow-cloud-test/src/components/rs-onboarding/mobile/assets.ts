import { assetPath } from "../asset-path";
export const mobileAssets = {
  iphoneBezel: assetPath("/assets/iphone/Bezel.png"),
  iphoneKeyboard: assetPath("/assets/iphone/Keyboard.png"),
  androidKeyboard: assetPath("/assets/android/Keyboard.png"),
  pixel10Bezel: assetPath("/assets/android/Pixel10.png"),
} as const;
