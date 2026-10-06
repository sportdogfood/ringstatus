import { type PropsWithChildren, useEffect, useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import { useDrag } from "@use-gesture/react";
import { AnimatePresence, motion } from "motion/react";
import { useKeyboard, useKeyboardInsets } from "./Keyboard";
import { useScreenPortal } from "./PhoneFrame";
import { useMobileDevice } from "./Device";

type BottomSheetProps = PropsWithChildren<{
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description?: string;
  snap?: number;
  presentation?: "flyup" | "drawer";
}>;

export function BottomSheet({
  open,
  onOpenChange,
  title,
  description,
  snap = 0.72,
  presentation = "flyup",
  children,
}: BottomSheetProps) {
  const { device } = useMobileDevice();
  const { screenRef } = useScreenPortal();
  const keyboard = useKeyboard();
  const { keyboardHeight } = useKeyboardInsets();
  const [dragOffset, setDragOffset] = useState(0);
  const isDrawer = presentation === "drawer";
  const axisIndex = isDrawer ? 0 : 1;

  useEffect(() => {
    if (open) keyboard.hide();
  }, [open]);

  const handleOpenChange = (nextOpen: boolean) => {
    if (nextOpen) {
      keyboard.hide();
    }

    onOpenChange(nextOpen);
  };

  const bindDrag = useDrag(
    (state) => {
      const movement = state.movement[axisIndex];
      const velocity = state.velocity[axisIndex];
      const direction = state.direction[axisIndex];
      const nextOffset = Math.max(0, movement);

      if (!state.last) {
        setDragOffset(nextOffset);
        return;
      }

      const shouldClose = nextOffset > 96 || (velocity > 0.55 && direction > 0);
      setDragOffset(0);

      if (shouldClose) {
        onOpenChange(false);
      }
    },
    {
      axis: isDrawer ? "x" : "y",
      filterTaps: true,
    },
  );

  const sheetHeight = Math.round(device.geometry.screen.height * snap);
  const effectiveHeight = Math.max(260, sheetHeight - Math.min(keyboardHeight, 180));
  const sheetBottom =
    device.platform === "android"
      ? Math.max(device.geometry.safeArea.bottom, keyboardHeight)
      : keyboardHeight;
  const portalContainer = screenRef.current ?? undefined;

  return (
    <Dialog.Root open={open} onOpenChange={handleOpenChange}>
      {/* Keep the portal mounted after `open` flips so AnimatePresence can run
          the sheet and overlay exit animations before Radix removes them. */}
      <Dialog.Portal container={portalContainer} forceMount>
        <AnimatePresence>
          {open ? (
            <>
              <Dialog.Overlay asChild forceMount>
                <motion.div
                  className="sheet-overlay"
                  data-testid="sheet-overlay"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.16 }}
                />
              </Dialog.Overlay>
              <Dialog.Content asChild forceMount>
                <motion.div
                  className="bottom-sheet"
                  data-testid="bottom-sheet"
                  data-presentation={presentation}
                  style={{
                    bottom: sheetBottom,
                    maxHeight: effectiveHeight,
                  }}
                  initial={isDrawer ? { x: device.geometry.screen.width + 36, y: 0 } : { y: effectiveHeight + 36, x: 0 }}
                  animate={isDrawer ? { x: dragOffset, y: 0 } : { y: dragOffset, x: 0 }}
                  exit={{
                    ...(isDrawer ? { x: device.geometry.screen.width + 36, y: 0 } : { y: effectiveHeight + 36, x: 0 }),
                    transition: {
                      type: "spring",
                      stiffness: 250,
                      damping: 30,
                      mass: 1.05,
                    },
                  }}
                  transition={{
                    type: "spring",
                    stiffness: 500,
                    damping: 43,
                    mass: 0.9,
                  }}
                >
                  <div className="sheet-handle-zone" data-testid="sheet-handle" {...bindDrag()}>
                    <div className="sheet-handle" />
                  </div>
                  <div className="sheet-header" {...(isDrawer ? bindDrag() : {})} style={isDrawer ? {touchAction: "pan-y"} : undefined}>
                    <Dialog.Title className="sheet-title">{title}</Dialog.Title>
                    {description ? <Dialog.Description className="sheet-description">{description}</Dialog.Description> : null}
                  </div>
                  <div className="sheet-content">{children}</div>
                </motion.div>
              </Dialog.Content>
            </>
          ) : null}
        </AnimatePresence>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
