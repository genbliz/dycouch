// https://stackoverflow.com/questions/40510611/typescript-interface-require-one-of-two-properties-to-exist

// type TypeFallBack<T> = undefined extends T ? Exclude<T, undefined> : T;
// type Complete<T> = {
//   [P in keyof Required<T>]: Pick<T, P> extends Required<Pick<T, P>> ? T[P] : T[P] | undefined;
// };

type RequireAtLeastOne<T, Keys extends keyof T = keyof T> = Pick<T, Exclude<keyof T, Keys>> &
  {
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>;
  }[Keys];

type RequireOnePropertyOnly<T, Keys extends keyof T = keyof T> = Pick<T, Exclude<keyof T, Keys>> &
  {
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Record<Exclude<Keys, K>, undefined>>;
  }[Keys];

interface MenuItem {
  title: string;
  component: number;
  click: number;
  icon: string;
}

type ClickOrComponent = RequireAtLeastOne<MenuItem, keyof MenuItem>;

const gggr: ClickOrComponent = {
  click: 0,
  component: 0,
};
