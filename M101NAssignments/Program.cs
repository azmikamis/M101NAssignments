using System;
using Assignments;

namespace m101n
{
    class Program
    {
        static void Main(string[] args)
        {
            Context context = new Context(new Week2Assignment());
            context.doAssignment();
            Console.WriteLine("Press Enter");
            Console.ReadLine();
        }
    }
}
