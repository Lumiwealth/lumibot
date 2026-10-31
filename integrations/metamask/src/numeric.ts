import { Decimal as DecimalConstructor } from "decimal.js";

// Keep token base-unit conversion exact without changing the SDK's global Decimal configuration.
export const Decimal = DecimalConstructor.clone({ precision: 256 });
